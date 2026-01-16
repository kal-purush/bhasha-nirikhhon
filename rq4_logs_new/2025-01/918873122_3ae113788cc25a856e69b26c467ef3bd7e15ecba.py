import streamlit as st
import pandas as pd
import plotly.express as px

# Load the datasets
steel_data = pd.read_csv('steel_combined_sensors_hourly.csv')
svr_predictions = pd.read_csv('svr_predictions.csv')

# Dashboard Title
st.title("Grid Guard")

# Layout for Sensor Panels
col1, col2, col3 = st.columns(3)

# Initial Graph: Current Over Time
with col1:
    st.subheader("ACS712: Current Sensor")
    
    # Default: Actual Current (from steel_combined_sensors_hourly.csv)
    current_value = steel_data["Current (A)"].iloc[-1]
    current_delta = current_value - steel_data['Current (A)'].iloc[-2]
    st.metric(
        label="Current (A)", 
        value=f"{current_value:.2f}", 
        delta=f"{current_delta:.2f}", 
        delta_color="normal"
    )
    
    # Plot the graph (based on initial state, showing actual current)
    fig_current = px.line(steel_data, x="Timestamp", y="Current (A)", title="Current Over Time")
    st.plotly_chart(fig_current, use_container_width=True)

# Initial Graph: CO Concentration Over Time
with col2:
    st.subheader("MQ-7: CO Sensor")
    
    # Default: Actual CO Concentration (from steel_combined_sensors_hourly.csv)
    co_value = steel_data["CO Concentration (ppm)"].iloc[-1]
    co_delta = co_value - steel_data["CO Concentration (ppm)"].iloc[-2]
    st.metric(
        label="CO (ppm)", 
        value=f"{co_value:.2f}", 
        delta=f"{co_delta:.2f}", 
        delta_color="normal"
    )
    
    # Plot the graph (based on initial state, showing actual CO concentration)
    fig_co = px.line(steel_data, x="Timestamp", y="CO Concentration (ppm)", title="CO Levels Over Time")
    st.plotly_chart(fig_co, use_container_width=True)

# Initial Graph: CO2 Concentration Over Time
with col3:
    st.subheader("MH-Z19: CO2 Sensor")
    
    # Default: Actual CO2 Concentration (from steel_combined_sensors_hourly.csv)
    co2_value = steel_data["CO2 Concentration (ppm)"].iloc[-1]
    co2_delta = co2_value - steel_data["CO2 Concentration (ppm)"].iloc[-2]
    st.metric(
        label="CO2 (ppm)", 
        value=f"{co2_value:.2f}",
        delta=f"{co2_delta:.2f}", 
        delta_color="normal"
    )
    
    # Plot the graph (based on initial state, showing actual CO2 concentration)
    fig_co2 = px.line(steel_data, x="Timestamp", y="CO2 Concentration (ppm)", title="CO2 Levels Over Time")
    st.plotly_chart(fig_co2, use_container_width=True)

# Data Analysis & Decision-Making Section
st.subheader("Data Analysis & Decision-Making")
if co_value > 50:
    st.warning("High CO concentration detected! Immediate action is recommended.")
if co2_value > 1000:
    st.warning("Elevated CO2 levels detected! Optimize ventilation.")
if current_value > 20:
    st.warning("High current usage detected! Consider load balancing.")


# Action Selection
control_action = st.selectbox("Select Control Action:", ["None", "Reduce Load", "Optimize Ventilation", "Optimize HVAC"])

# Handle different control actions and update the graphs dynamically
if control_action == "Reduce Load":
    st.subheader("ACS712: Predicted Current")
    
    # Predicted current value
    predicted_current_value = svr_predictions["Predicted_Current"].iloc[-1]
    
    # Ensure the delta shows as negative if the predicted current is lower than the actual current
    predicted_current_delta = current_value - predicted_current_value  # The delta should be negative if predicted is lower
    
    # Display the metric with a negative delta (red downward arrow)
    st.metric(
        label="Predicted Current (A)", 
        value=f"{predicted_current_value:.2f}",
        delta=f"{predicted_current_delta:.2f}", 
        delta_color="inverse"  # Using inverse for downward arrow (red)
    )
    
    # Plot the graph (showing predicted current values)
    fig_current = px.line(svr_predictions, x="Timestamp", y="Predicted_Current", title="Predicted Current Over Time")
    st.plotly_chart(fig_current, use_container_width=True)

    # Warnings and Recommendations
    if predicted_current_value > 20:
        st.warning("High predicted current usage detected! Consider load balancing.")
        
elif control_action == "Optimize Ventilation":
    st.subheader("MQ-7: Predicted CO Concentration")
    
    # Predicted CO value
    predicted_co_value = svr_predictions["Predicted_CO"].iloc[-1]
    
    # Ensure the delta shows as negative if the predicted CO is lower than the actual CO
    predicted_co_delta = co_value - predicted_co_value  # The delta should be negative if predicted is lower
    st.metric(
        label="Predicted CO (ppm)", 
        value=f"{predicted_co_value:.2f}",
        delta=f"{predicted_co_delta:.2f}", 
        delta_color="inverse"  # Using inverse for downward arrow (red)
    )
    
    # Plot the graph (showing predicted CO values)
    fig_co = px.line(svr_predictions, x="Timestamp", y="Predicted_CO", title="Predicted CO Levels Over Time")
    st.plotly_chart(fig_co, use_container_width=True)

    # Warnings and Recommendations
    if predicted_co_value > 800:
        st.warning("High predicted CO concentration detected! Immediate action is recommended.")
        
elif control_action == "Optimize HVAC":
    st.subheader("MH-Z19: Predicted CO2 Concentration")
    
    # Predicted CO2 value
    predicted_co2_value = svr_predictions["Predicted_CO2"].iloc[-1]
    
    # Ensure the delta shows as negative if the predicted CO2 is lower than the actual CO2
    predicted_co2_delta = co2_value - predicted_co2_value  # The delta should be negative if predicted is lower
    st.metric(
        label="Predicted CO2 (ppm)", 
        value=f"{predicted_co2_value:.2f}",
        delta=f"{predicted_co2_delta:.2f}", 
        delta_color="inverse"  # Using inverse for downward arrow (red)
    )
    
    # Plot the graph (showing predicted CO2 values)
    fig_co2 = px.line(svr_predictions, x="Timestamp", y="Predicted_CO2", title="Predicted CO2 Levels Over Time")
    st.plotly_chart(fig_co2, use_container_width=True)

    # Warnings and Recommendations
    if predicted_co2_value > 1000:
        st.warning("Elevated predicted CO2 levels detected! Optimize ventilation.")

# Generate Report Section
st.subheader("Generate Report")

# Report Generation based on current data or predicted values
if st.button("Generate Report"):
    report_data = {
        "Sensor": ["ACS712", "MQ-7", "MH-Z19"],
        "Metric": ["Current (A)", "CO (ppm)", "CO2 (ppm)"],
        "Latest Value": [
            f"{predicted_current_value:.2f}" if control_action == "Reduce Load" else f"{current_value:.2f}",
            f"{predicted_co_value:.2f}" if control_action == "Optimize Ventilation" else f"{co_value:.2f}",
            f"{predicted_co2_value:.2f}" if control_action == "Optimize HVAC" else f"{co2_value:.2f}"
        ],
        "Status": [
            "High" if (control_action == "Reduce Load" and predicted_current_value > 20) else ("Normal"),
            "High" if (control_action == "Optimize Ventilation" and predicted_co_value > 50) else ("Normal"),
            "High" if (control_action == "Optimize HVAC" and predicted_co2_value > 1000) else ("Normal")
        ]
    }
    report_df = pd.DataFrame(report_data)
    
    st.write(report_df)
    
    # Allow the user to download the report as CSV
    st.download_button("Download Report", data=report_df.to_csv(index=False), file_name="emissions_report.csv", mime="text/csv")

# Footer
st.markdown("---")
st.caption("Energy Emissions Dashboard | Developed with Streamlit")





