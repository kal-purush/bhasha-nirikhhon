import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

# Load dataset
df = pd.read_csv("doctor_performance.csv")
df['Consultation Date'] = pd.to_datetime(df['Consultation Date'])

# Initialize Dash app
app = Dash(__name__)
app.title = "Myayurhealth"

# App layout
app.layout = html.Div([
    # Add logo image at the top
    html.Div([
        html.Img(src="https://myayurhealth.com/assets/img/ayur/my-ayur-logo-retina.png", 
                 style={"height": "80px", "display": "block", "margin": "0 auto"}),
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # Title
    html.H1("Doctor Performance Analytics Dashboard", style={"textAlign": "center"}),

    # Filters
    html.Div([
        html.Div([
            html.Label("Select Doctor(s):"),
            dcc.Dropdown(
                id="doctor-dropdown",
                options=[{"label": doc, "value": doc} for doc in df["Doctor"].unique()],
                multi=True,
                placeholder="Select Doctor(s)"
            ),
        ], style={"flex": "1"}),

        html.Div([
            html.Label("Select Date Range:"),
            dcc.DatePickerRange(
                id="date-picker",
                start_date=df["Consultation Date"].min(),
                end_date=df["Consultation Date"].max(),
                display_format="YYYY-MM-DD"
            ),
        ], style={"flex": "1"})
    ], style={"display": "flex", "gap": "10px", "marginBottom": "20px"}),

    # Summary Metrics
    html.Div(id="summary-metrics", style={"display": "flex", "gap": "20px", "justifyContent": "space-around"}),

    # Visualizations
    dcc.Graph(id="consultation-volume"),
    dcc.Graph(id="patient-satisfaction"),
    dcc.Graph(id="treatment-efficacy"),
    dcc.Graph(id="response-time"),
    dcc.Graph(id="retention-rate"),
    dcc.Graph(id="revenue-metrics"),
])

# Callbacks for interactivity
@app.callback(
    [
        Output("summary-metrics", "children"),
        Output("consultation-volume", "figure"),
        Output("patient-satisfaction", "figure"),
        Output("treatment-efficacy", "figure"),
        Output("response-time", "figure"),
        Output("retention-rate", "figure"),
        Output("revenue-metrics", "figure"),
    ],
    [
        Input("doctor-dropdown", "value"),
        Input("date-picker", "start_date"),
        Input("date-picker", "end_date"),
    ]
)
def update_dashboard(selected_doctors, start_date, end_date):
    # Filter data
    filtered_df = df[(df["Consultation Date"] >= start_date) & (df["Consultation Date"] <= end_date)]
    if selected_doctors:
        filtered_df = filtered_df[filtered_df["Doctor"].isin(selected_doctors)]
    
    # Summary Metrics
    total_consultations = len(filtered_df)
    avg_satisfaction = filtered_df["Feedback"].mean()
    success_rate = (filtered_df["Outcome"].value_counts(normalize=True).get("Successful", 0)) * 100
    avg_response_time = filtered_df["Response Time"].mean()
    retention_rate = (filtered_df["Retention"].mean()) * 100
    total_revenue = filtered_df["Revenue"].sum()

    metrics = [
        html.Div(f"Total Consultations: {total_consultations}", style={"fontSize": "20px", "textAlign": "center"}),
        html.Div(f"Avg. Patient Satisfaction: {avg_satisfaction:.2f}", style={"fontSize": "20px", "textAlign": "center"}),
        html.Div(f"Treatment Efficacy: {success_rate:.2f}%", style={"fontSize": "20px", "textAlign": "center"}),
        html.Div(f"Avg. Response Time: {avg_response_time:.2f} mins", style={"fontSize": "20px", "textAlign": "center"}),
        html.Div(f"Retention Rate: {retention_rate:.2f}%", style={"fontSize": "20px", "textAlign": "center"}),
        html.Div(f"Total Revenue: ${total_revenue:.2f}", style={"fontSize": "20px", "textAlign": "center"}),
    ]

    # Visualizations
    consultation_volume = px.bar(filtered_df, x="Consultation Date", color="Doctor", title="Consultation Volume Over Time")
    patient_satisfaction = px.bar(filtered_df.groupby("Doctor", as_index=False)["Feedback"].mean(), x="Doctor", y="Feedback", title="Avg. Patient Satisfaction by Doctor")
    treatment_efficacy = px.pie(filtered_df, names="Outcome", title="Treatment Efficacy Distribution", hole=0.4)
    response_time = px.bar(filtered_df.groupby("Doctor", as_index=False)["Response Time"].mean(), x="Doctor", y="Response Time", title="Avg. Response Time by Doctor")
    retention_rate = px.bar(filtered_df.groupby("Doctor", as_index=False)["Retention"].mean(), x="Doctor", y="Retention", title="Patient Retention Rate by Doctor")
    revenue_metrics = px.bar(filtered_df.groupby("Doctor", as_index=False)["Revenue"].sum(), x="Doctor", y="Revenue", title="Total Revenue by Doctor")

    return metrics, consultation_volume, patient_satisfaction, treatment_efficacy, response_time, retention_rate, revenue_metrics

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)