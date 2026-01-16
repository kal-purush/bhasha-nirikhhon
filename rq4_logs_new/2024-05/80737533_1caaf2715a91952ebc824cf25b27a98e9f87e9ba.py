import plotly.express as px
import plotly
import pandas as pd
import plotly.figure_factory as ff

# Read dataframe from Excel
df = pd.read_excel("capacity.xlsx")

# Assign columns to vars
prs = df['Task']
start = df['Start']
finish = df['Finish']
completed = df['completed %']

# Create Gantt
fig = px.timeline(df, 
                  x_start=start, 
                  x_end=finish, 
                  y=prs, 
                  color=completed, 
                  title='B24',
                  color_continuous_scale=[(0, "red"), (0.5, "blue"), (1, "green")])

# Layout
fig.update_yaxes(autorange='reversed')
fig.update_layout(
    title_font_size=42,
    font_size=18,
    title_font_family='Arial'
)

# Interactive Gantt
#fig = ff.create_gantt(df)

plotly.offline.plot(fig, filename='capacity.html')