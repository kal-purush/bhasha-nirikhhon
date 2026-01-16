import streamlit as st
import google.generativeai as palm
import os
from ics import Calendar, Event
from datetime import datetime, timedelta
import json

st.set_page_config(page_title="TraWell", page_icon = "🌏")
st.title("TraWell")
# Loading Image using PIL
# Adding Image to web app
with st.sidebar:
 st.image("TraWell.png", width=270)
 left_co, cent_co,last_co = st.columns(3)
 st.text('Welcome to TraWell! \n A personalized weekend getaway \n itinerary generator for all your \n traveling wants and needs!\n Enter your preferences to the \n right  and happy traveling!')


palm.configure(api_key=os.getenv("PALM_API_KEY"))
models = [m for m in palm.list_models() if 'generateText' in m.supported_generation_methods]
model = models[0].name
print(model)

city = st.text_input("Enter the city you're visiting:")
days = st.number_input("Enter the duration of your trip: ", min_value=1, max_value=3)
days = int(days)
budget = st.slider("Select your budget: ", min_value=50, max_value=1000, step=50)
budget = int(budget)

# User preferences checkboxes
st.text("Which are most important to you?")
art = st.checkbox("Art")
nature = st.checkbox("Nature")
music = st.checkbox("Music")
indoor = st.checkbox("Indoor Activities")
food = st.checkbox("Food")
history = st.checkbox("History")
acc = st.checkbox("Accessibility accomodations available")

# Generate itinerary button
if st.button("Generate Itinerary"):
    # Create a prompt based on user input
    
        prompt = f"You are an travel expert. Give me an itenary for {city}, for {days} days, assume each day strictly starts at 10am and ends at 8pm with a buffer of 30 minutes between each activity. Pick activities based on the budget and ensure the total of all activities does NOT exceed the strict budget of {budget}. I like "
        if art:
            prompt += " art,"
        if nature:
            prompt += " nature,"
        if music:
            prompt += " music,"
        if indoor:
            prompt += " indoor activities,"
        if food:
            prompt += " food,"
        if history:
            prompt += " history,"
        if acc:
            prompt += " accessibility accomodations,"

        if prompt.endswith(","):
            prompt = prompt[:-1]

        # prompt += ". Return the response in a properly formatted json string which can be imported in code using json.loads function in python."
        prompt += """Limit the length of output json string to 10000 characters. Generate a structured JSON representation for the travel itinerary.

            {
            "days": [
                {
                    "day": 1,
                    "activities": [
                        {
                            "title": "Activity 1",
                            "description": "Description of Activity 1",
                            "link": "https://example.com/activity1",
                            "start_time": "10:00 AM",
                            "end_time": "12:00 PM",
                            "location": "https://maps.google.com/?q=location1",
                            "cost": "Price of Activity 1"
                            "accessibility": "overall accessibility rating with type of accomodation for Activity 1"
                        },
                        {
                            "title": "Activity 2",
                            "description": "Description of Activity 2",
                            "link": "https://example.com/activity2",
                            "start_time": "02:00 PM",
                            "end_time": "04:00 PM",
                            "location": "https://maps.google.com/?q=location2",
                            "cost": "Price of Activity 2"
                            "accessibility": "overall accessibility rating with type of accomodation for Activity 2"

                        }
                    ]
                }
            ]
        }


            Ensure that each day has a "day" field and a list of 'activities' with 'title', 'description', 'start_time', 'end_time', 'location', 'cost', and, if applicable, 'accessibility' fields. Keep descriptions concise yet detailed.
    """

        # Call the Palm API
        completion = palm.generate_text(
        model=model,
        prompt=prompt,
        temperature=0,
        # The maximum length of the response
        max_output_tokens=10000,
    )

        # Extract and display the generated itinerary
        itinerary = completion.result.strip()
        itinerary = itinerary[7:-3]

        # Display the itinerary from the JSON response
        print(type(itinerary))
        print(len(itinerary))
        print(itinerary)

        itinerary_json = json.loads(itinerary)

        for day in itinerary_json["days"]:
            st.header(f"Day {day['day']}")
            for activity in day["activities"]:
                st.subheader(activity["title"])
                st.write(f"Description: {activity['description']}")
                st.write(f"Location: {activity['location']}")
                st.write(f"Time: {activity['start_time']} - {activity['end_time']}")
                st.write(f"Link: {activity['link']}")
                st.write(f"Cost: {activity['cost']}")  
                if acc:
                    st.write(f"Accessibility rating: {activity['accessibility']}")
                else:
                    pass
                st.write("\n")
        
        # Set the start date to tomorrow
        start_date = datetime.now() + timedelta(days=1)

        # Create a download link for the generated itinerary
        def get_download_link(content, filename):
            # Encode content as base64
            b64_content = content.encode().decode("utf-8")
            # Generate the download link
            href = f'<a href="data:text/calendar;charset=utf-8,{b64_content}" download="{filename}">Download {filename}</a>'
            return href


        cal = Calendar()
        start_date = datetime.now() + timedelta(days=1)


        for day_data in itinerary_json["days"]:
            for activity in day_data.get("activities", []):
                event = Event()
                event.name = activity.get("title", "")
                event.description = activity.get("description", "")
                event.location = activity.get("location", "")
                event.begin = start_date + timedelta(days=day_data["day"] - 1, hours=0, minutes=0)
                event.end = start_date + timedelta(days=day_data["day"] - 1, hours=23, minutes=59)
                cal.events.add(event)

        cal_content = str(cal)

        # Display success message and download link
        st.success("Itinerary ready to export!")
        st.markdown(get_download_link(cal_content, "Itinerary.ics"), unsafe_allow_html=True)

    # except Exception as e:
    #     st.error(f"There was an error in generating your trip! {e}")
    #     print(Exception)