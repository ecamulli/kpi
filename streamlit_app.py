import requests
import pandas as pd
from datetime import datetime
from xlsxwriter import Workbook
from time import sleep
from random import uniform
from typing import List, Dict, Optional
from pathlib import Path
import logging
import streamlit as st
from ratelimit import limits, sleep_and_retry
import json
from io import BytesIO

# ========== CONFIG ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

AUTH_URL = "https://api-v2.7signal.com/oauth2/token"
BASE_URL = "https://api-v2.7signal.com"
NETWORKS_URL = f"{BASE_URL}/networks/sensors"
AVERAGING = "TENMIN"
TIMELIMIT = "ONEHOUR"
CALLS = 30  # Set to 30
PERIOD = 5  # Set to 5 seconds
VALID_BANDS = {"2.4", "5.0", "5", "6.0", "6"}  # Valid bands for input validation
DISPLAY_BANDS = ["2.4", "5.0", "6.0"]  # Fixed order for display

# ========== AUTH FUNCTION ==========
def get_auth_token(client_id: str, client_secret: str) -> Optional[str]:
    """Fetch an OAuth2 access token using client credentials."""
    auth_data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    auth_headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    try:
        response = requests.post(AUTH_URL, data=auth_data, headers=auth_headers, timeout=10)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.RequestException as e:
        logger.error(f"Auth error for client {client_id}: {e}")
        return None

# ========== GET NETWORKS ==========
def get_networks(session: requests.Session) -> List[str]:
    """Fetch all network names from the API."""
    try:
        response = session.get(NETWORKS_URL, timeout=10)
        response.raise_for_status()
        networks = response.json().get("results", [])
        network_names = [network.get("name", "").strip() for network in networks if network.get("name")]
        return sorted(set(network_names))  # Remove duplicates and sort
    except requests.RequestException as e:
        logger.error(f"Failed to fetch networks: {e}")
        return []

# ========== GET ACCESS POINTS ==========
def get_access_points(session: requests.Session) -> List[Dict]:
    """Fetch all access points from the API."""
    url = f"{BASE_URL}/access-points/sensors"
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch access points: {e}")
        return []

# ========== GET KPI DATA PER AP ==========
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_ap_kpi(ap_id: str, ap_name: str, session: requests.Session, kpi_code: str, retries: int = 3) -> Dict:
    """Fetch KPI data for a specific access point."""
    url = f"{BASE_URL}/kpis/sensors/access-points/{ap_id}?kpiCodes={kpi_code}&averaging={AVERAGING}&timelimit={TIMELIMIT}"
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                sleep_time = float(retry_after) if retry_after else (2 ** attempt) * 2 + uniform(0, 0.2)
                logger.warning(f"Rate limit hit for AP {ap_name}, retrying in {sleep_time:.2f}s...")
                sleep(sleep_time)
                continue
            response.raise_for_status()
            logger.debug(f"Rate limit headers: {response.headers}")
            data = response.json()
            if "results" not in data or not data["results"]:
                logger.warning(f"No KPI results for AP {ap_name}")
                return {}
            results = data["results"]
            measurements = results[0].get("measurements5GHz", [])
            if not measurements:
                logger.warning(f"No KPI measurements for AP {ap_name}")
                return {}
            avg_kpi = sum(m["kpiValue"] for m in measurements) / len(measurements)
            return {
                "Avg KPI Value": round(avg_kpi, 2),
                "Latest Status": measurements[-1].get("status"),
            }
        except requests.RequestException as e:
            logger.error(f"Request error for AP {ap_name}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Parsing error for AP {ap_name}: {e}")
            return {}
    return {}

# ========== PROCESS APS ==========
def process_access_points(session: requests.Session, target_networks: set, target_bands: set, kpi_code: str) -> List[Dict]:
    """Fetch and process access points, return results."""
    access_points = get_access_points(session)
    
    # Filter access points by network name and band
    valid_aps = [
        ap for ap in access_points
        if isinstance(ap, dict) and "id" in ap and "network" in ap and "band" in ap
        and ap["network"] and ap["network"].strip('"').lower() in target_networks
        and ap["band"] and (
            band := str(ap["band"]).lower().replace("ghz", ""),
            band := "5.0" if band == "5" else "6.0" if band == "6" else band,
            band in target_bands
        )[1]
    ]
    
    if not valid_aps:
        logger.warning(f"No access points found for networks: {', '.join(target_networks)} and bands: {', '.join(target_bands)}")
        return []

    logger.info(f"Found {len(valid_aps)} access points for specified networks and bands.")
    results = []

    for ap in valid_aps:
        sleep(0.5)  # Stagger requests
        ap_name = ap.get("name", "Unknown")
        kpi_data = get_ap_kpi(ap["id"], ap_name, session, kpi_code)
        bssid = ap.get("bssid", "Unknown")
        status = kpi_data.get("Latest Status", "N/A")
        band = ap.get("band", "Unknown")
        network = ap.get("network", "Unknown").strip('"')
        logger.info(f"AP: {ap_name:<30} | BSSID: {bssid:<17} | Status: {status}")

        result = {
            "Access Point Name": ap_name,
            "BSSID": bssid,
            "Service Area": ap.get("serviceAreaName"),
            "Band": band,
            "Network": network,
            **kpi_data
        }
        results.append(result)

    return results

# ========== STREAMLIT APP ==========
def main():
    st.title("Access Point KPI Dashboard")
    st.markdown("Enter your credentials and parameters to fetch access point KPI data for the last hour.")

    # Input fields
    account_name = st.text_input("Customer Name")
    client_id = st.text_input("Client ID")
    client_secret = st.text_input("Client Secret", type="password")
    kpi_code = st.text_input("KPI Code")

    # Initialize session state for networks and results
    if "networks" not in st.session_state:
        st.session_state.networks = []
    if "results" not in st.session_state:
        st.session_state.results = None

    # Authenticate and fetch networks
    if client_id and client_secret:
        token = get_auth_token(client_id, client_secret)
        if token:
            with requests.Session() as session:
                session.headers.update({"Authorization": f"Bearer {token}"})
                st.session_state.networks = get_networks(session)
                st.session_state.session = session  # Store session for later use
        else:
            st.error("Authentication failed. Please check your Client ID and Client Secret.")
            return
    else:
        st.warning("Please enter Client ID and Client Secret to fetch available networks.")
        return

    # Network and band selection
    if st.session_state.networks:
        target_networks = st.multiselect(
            "Select Networks",
            options=st.session_state.networks,
            help=f"Choose from available networks: {', '.join(st.session_state.networks)}"
        )
    else:
        st.error("No networks available. Please check your credentials or API connectivity.")
        return

    target_bands = st.multiselect(
        "Select Bands",
        options=DISPLAY_BANDS,
        help=f"Choose from available bands: {', '.join(DISPLAY_BANDS)}"
    )

    # Run button
    if st.button("Fetch KPI Data"):
        if not all([account_name, client_id, client_secret, kpi_code, target_networks, target_bands]):
            st.error("All fields (Customer Name, Client ID, Client Secret, KPI Code, Networks, Bands) must be provided.")
            return

        target_networks = {n.lower() for n in target_networks}
        target_bands = {b.lower() for b in target_bands if b.lower() in VALID_BANDS}

        if not target_bands.issubset(VALID_BANDS):
            st.error(f"Invalid bands selected. Valid bands are: {', '.join(DISPLAY_BANDS)}")
            return

        with st.spinner("Fetching KPI data..."):
            results = process_access_points(st.session_state.session, target_networks, target_bands, kpi_code)
            st.session_state.results = results

        if results:
            df = pd.DataFrame(results)
            st.success("Data fetched successfully!")
            st.subheader("Results")
            st.dataframe(df)

            # Generate Excel file in memory
            today_str = datetime.today().strftime("%Y-%m-%d")
            excel_filename = f"{account_name}_access_point_kpi_summary_{today_str}.xlsx"
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, sheet_name="AP KPI Summary", index=False)
                worksheet = writer.sheets["AP KPI Summary"]
                for i, col in enumerate(df.columns):
                    column_len = max(len(col), df[col].astype(str).map(len).max())
                    worksheet.set_column(i, i, min(column_len + 2, 50))
            excel_data = output.getvalue()

            # Generate JSON file in memory
            json_data = json.dumps(results, indent=2).encode('utf-8')

            # Download buttons
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="Download Excel",
                    data=excel_data,
                    file_name=excel_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            with col2:
                st.download_button(
                    label="Download JSON",
                    data=json_data,
                    file_name="access_point_kpi.json",
                    mime="application/json"
                )
        else:
            st.warning("No data found for the selected networks and bands.")

if __name__ == "__main__":
    main()
