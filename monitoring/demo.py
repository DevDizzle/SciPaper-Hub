import streamlit as st
import pandas as pd
import requests
import json
import plotly.express as px
from collections import Counter

st.markdown("""
    <style>
    h1, h2, h3, h4, h5, h6 {
        color: #FBF3FA !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- Configuration ---
API_URL = "https://paperrec-search-550651297425.us-central1.run.app/search"
LOG_FILE = "monitoring/simulation_logs.json"

st.set_page_config(page_title="SciPaper-Hub Command Center", layout="wide")
st.title("🔭 SciPaper-Hub: MLOps Command Center")

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["🚀 Live Recommender", "📊 A/B Experiment Results", "⚖️ Fairness Monitor"])

# --- Tab 1: Live System ---
with tab1:
    st.header("Real-Time Paper Recommendation")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        query_url = st.text_input("Enter arXiv URL:", "https://arxiv.org/abs/2511.09414")
        k_val = st.slider("Number of Recommendations (k):", 1, 10, 5)
        
        if st.button("Get Recommendations", type="primary"):
            try:
                with st.spinner("Embedding abstract & querying Vertex AI..."):
                    # Call your actual API
                    response = requests.post(API_URL, json={"url": query_url, "k": k_val})
                    
                if response.status_code == 200:
                    data = response.json()
                    st.success(f"Success! Found {len(data['neighbors'])} papers.")
                    
                    # Display results beautifully
                    for idx, paper in enumerate(data['neighbors']):
                        with st.expander(f"#{idx+1} - {paper['id']} (Dist: {paper['distance']:.4f})"):
                            st.markdown("**Metadata:**")
                            st.json(paper.get('metadata', {}))
                else:
                    st.error(f"Error {response.status_code}: {response.text}")
            except requests.exceptions.ConnectionError:
                st.error("🚨 Could not connect to API. Is 'uvicorn' running?")

    with col2:
        st.info("ℹ️ **System Status**\n\n* **Service:** Cloud Run\n* **Index:** Vertex AI Vector Search\n* **Embedding:** Text-Embedding-005")

# --- Tab 2: A/B Test Results ---
with tab2:
    st.header("A/B Experiment Telemetry")
    
    try:
        # Load your existing simulation logs
        with open(LOG_FILE, 'r') as f:
            raw_logs = json.load(f)
            
        # Extract relevant fields
        logs_data = []
        for entry in raw_logs:
            payload = entry.get('jsonPayload', {})
            if payload.get('message') == 'RECO_RESPONSE':
                logs_data.append({
                    'request_id': payload.get('request_id'),
                    'user_group': payload.get('user_group'),
                    'model_version': payload.get('model_version'),
                    'recs_returned': len(payload.get('recommendations', [])),
                    'timestamp': entry.get('timestamp')
                })
        
        df = pd.DataFrame(logs_data)
        
        # Metrics Row
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Requests", len(df))
        m2.metric("Traffic Group A", len(df[df['user_group'] == 'A']))
        m3.metric("Traffic Group B", len(df[df['user_group'] == 'B']))
        
        # Charts
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Traffic Distribution by User Group")
            fig_traffic = px.pie(df, names='user_group', title="User Group Split (A vs B)")
            st.plotly_chart(fig_traffic, use_container_width=True)
            
        with c2:
            st.subheader("Model Version Usage")
            fig_model = px.bar(df, x='model_version', title="Requests Served by Model Version")
            st.plotly_chart(fig_model, use_container_width=True)

        st.markdown("### Raw Telemetry Logs")
        st.dataframe(df.sort_values('timestamp', ascending=False))

    except FileNotFoundError:
        st.warning(f"Could not find `{LOG_FILE}`. Run your simulation script first!")

# --- Tab 3: Fairness & Loop Analysis ---
with tab3:
    st.header("Fairness & Feedback Loop Detection")
    
    if 'raw_logs' in locals():
        # Extract all recommended paper IDs to check for popularity loops
        all_recs = []
        for entry in raw_logs:
            recs = entry.get('jsonPayload', {}).get('recommendations', [])
            all_recs.extend(recs)
            
        rec_counts = Counter(all_recs)
        rec_df = pd.DataFrame(rec_counts.items(), columns=['Paper ID', 'Frequency']).sort_values('Frequency', ascending=False)
        
        col_f1, col_f2 = st.columns([2, 1])
        
        with col_f1:
            st.subheader("Popularity Bias Detection")
            st.markdown("Analyzing the frequency of paper appearances in search results.")
            fig_bias = px.bar(rec_df.head(10), x='Paper ID', y='Frequency', 
                              title="Top 10 Most Recommended Papers (Potential Feedback Loop)")
            st.plotly_chart(fig_bias, use_container_width=True)
            
        with col_f2:
            st.subheader("Bias Metrics")
            top_1_percent = rec_df.iloc[0]['Frequency'] / len(all_recs)
            st.metric("Top-1 Exposure Rate", f"{top_1_percent:.1%}", help="Percentage of all slots occupied by the #1 paper")
            
            unique_ratio = len(rec_df) / len(all_recs)
            st.metric("Catalog Coverage", f"{unique_ratio:.1%}", help="Percentage of unique papers vs total slots")
            
            if top_1_percent > 0.10:
                st.error("🚨 **High Bias Detected:** The top paper appears in >10% of results.")
            else:
                st.success("✅ **Bias Nominal:** Exposure is well distributed.")
