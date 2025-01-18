import requests
import pandas as pd
import streamlit as st

# Bright Data API Credentials
API_TOKEN = "4b1e1e57-2f5c-4c4c-9f46-13ebc7fe2a64"
DATASET_ID = "gd_lfqkr8wm13ixtbd8f5"

# API Endpoints
TRIGGER_URL = f"https://api.brightdata.com/datasets/v3/trigger?dataset_id={DATASET_ID}&include_errors=true&type=discover_new&discover_by=input_filters"
SNAPSHOT_URL = "https://api.brightdata.com/datasets/v3/snapshot/"
SNAPSHOT_READY_URL = f"https://api.brightdata.com/datasets/v3/snapshots?dataset_id={DATASET_ID}&status=ready"

# Function to trigger scraping
def trigger_scraping(location, listing_category, home_type):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }
    
    payload = [
        {
            "location": location,
            "listingCategory": listing_category,
            "HomeType": home_type if home_type else None,
        }
    ]
    
    try:
        response = requests.post(TRIGGER_URL, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()
        return result.get("snapshot_id")
    except requests.RequestException as e:
        st.error(f"Failed to trigger scraping: {e}")
        st.error(f"Response: {response.content.decode('utf-8')}")
        return None

# Function to parse and extract desired attributes from API response
def parse_property_data(raw_data):
    parsed_data = []
    for item in raw_data:
        property_info = {
            "price": item.get("price"),
            "beds": item.get("beds"),
            "baths": item.get("baths"),
            "address": item.get("address"),
            "property_link": item.get("property_link"),
            "sqft": item.get("sqft"),
            "home_type": item.get("home_type"),
            "lot_size": item.get("lot_size"),
            "hoa_fee": item.get("hoa_fee"),
            "estimated_mortgage_value": item.get("estimated_mortgage_value"),
        }
        parsed_data.append(property_info)
    return parsed_data

# Function to fetch snapshot data
def fetch_snapshot_data(snapshot_id):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
    }
    try:
        url = f"{SNAPSHOT_URL}{snapshot_id}?format=json"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        raw_data = response.json()

        # Parse the response to extract required attributes
        if isinstance(raw_data, list):
            return parse_property_data(raw_data)
        elif isinstance(raw_data, dict):
            return parse_property_data([raw_data])
        else:
            return []
    except requests.RequestException as e:
        st.error(f"Failed to fetch snapshot data: {e}")
        return None

# Streamlit UI
st.title("Zillow Data Scraper")

# Form for user input
st.write("### Input Details")
with st.form(key="scraper_form"):
    location = st.text_input("Enter location (e.g., New York, 02118):", value="New York")
    listing_category = st.selectbox(
        "Listing Category:",
        ["House for sale", "House for rent"],
    )
    home_type = st.text_input(
        "Enter home type (e.g., Single-Family, Condo, leave blank for all):", value=""
    )
    submit = st.form_submit_button("Trigger Scraping")

# Trigger scraping on form submission
if submit:
    with st.spinner("Triggering scraping process..."):
        snapshot_id = trigger_scraping(location, listing_category, home_type)
        if snapshot_id:
            st.success("Scraping trigger successful!")
            st.write(f"Snapshot ID: {snapshot_id}")
            st.session_state["snapshot_id"] = snapshot_id
        else:
            st.error("Failed to trigger scraping. Check API response.")

# Display form for fetching data if snapshot ID exists
if "snapshot_id" in st.session_state:
    st.write("Enter the Snapshot ID to fetch data:")
    snapshot_id_input = st.text_input("Snapshot ID:", value=st.session_state["snapshot_id"])
    if st.button("Fetch Data"):
        with st.spinner("Fetching snapshot data..."):
            data = fetch_snapshot_data(snapshot_id_input)
            if data:
                df = pd.DataFrame(data)
                st.write(df)

                # Provide a download button for the parsed data
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download Data as CSV",
                    data=csv,
                    file_name="scraped_data.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No data available. Please check the snapshot ID.")
