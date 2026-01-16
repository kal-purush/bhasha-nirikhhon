import csv
import plotly.express as px
import pandas as pd

days = {
    "mon": "Monday",
    "tue": "Tuesday",
    "wed": "Wednesday",
    "thu": "Thursday",
    "fri": "Friday",
    "sat": "Saturday & Sunday",
}

exhibits = {
    "at": "Around Town",
    "htt": "Hit The Trail",
    "iw": "IdeaWorks",
    "kg": "Kid Grid",
    "mp": "Moneypalooza",
    "pz": "Putting Zoo",
    "p2p": "Power2Play",
    "rpw": "River Playway",
    "sea": "Sea Shapes",
    "sdl": "Seedlings",
    "sp": "Splash",
    "stm": "STEMOsphere",
    "sn": "Stepnotes",
    "sun": "Sun Sprouts",
    "th": "Toddlers Hollow",
}

tasks = {
    "dps": "Disinfect, Prop Swap",
    "wd": "Wipe Down While Open to Guests",
}

dates_for_days = {
    "wed": "2020-09-10",
    "thu": "2020-09-11",
    "fri": "2020-09-12",
    "sat": "2020-09-13",
    "sun": "2020-09-14",
}


def formatDate(weekday, time_of_day):
    if len(time_of_day) == 4:
        time = f"{time_of_day[0:2]}:{time_of_day[2:]}:00"
    else:
        time = f"{time_of_day[0]}:{time_of_day[1:2]}:00"
    date = f"{dates_for_days.get(str(weekday))} {time}"
    print(date)
    return date


with open("input.csv", "r") as input_file:
    reader = csv.DictReader(input_file)
    events = list(reader)
    for row in events:
        row["Start"] = formatDate(row["Day"], row["Start"])
        row["End"] = formatDate(row["Day"], row["End"])
        row["Task"] = tasks[row["Task"]]
        row["exhibit1"] = exhibits.get(row["exhibit1"])
        row["exhibit2"] = exhibits.get(row["exhibit2"])
        if row["exhibit2"]:
            row["Exhibits"] = " & ".join(row["exhibit1"], row["exhibit2"])
        else:
            row["Exhibits"] = row["exhibit1"]


print(events)

edf = pd.DataFrame(events)

df = pd.DataFrame(
    [
        dict(
            Task="DPS",
            Start="2020-09-12 10:00:00",
            Finish="2020-09-12 10:45:00",
            Exhibit="HTT",
        ),
        dict(
            Task="WD",
            Start="2020-09-12 10:45:00",
            Finish="2020-09-12 11:30:00",
            Exhibit="HTT",
        ),
        dict(
            Task="DPS",
            Start="2020-09-12 11:30:00",
            Finish="2020-09-12 12:15:00",
            Exhibit="AT",
        ),
    ]
)

fig = px.timeline(
    edf,
    x_start="Start",
    x_end="End",
    y="Exhibits",
    color="Task",
)
fig.update_yaxes(autorange="reversed")

fig.update_layout(title="Saturday Exhibit Cleaning", xaxis_tickformat="%H:%M")
fig.update_xaxes(tick0=0.25)
config = {"displayModeBar": True}
fig.show(config=config)