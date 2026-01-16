import pandas as pd
import streamlit as st
import requests
from datetime import datetime, timedelta

today = datetime.today().date()
tomorrow = today + timedelta(days=1)


weekday = today.weekday()  
days_until_saturday = (5 - weekday) % 7
saturday = today + timedelta(days=days_until_saturday)
sunday = saturday + timedelta(days=1)


# Initialize the dataframe
data = {
    "Listing_Name": [
        "Cozy Downtown Loft", "Luxury LA Villa with Pool", "Chic Chicago Apartment",
        "Beachside Condo in Miami", "Modern Dallas Studio", "Bayview SF Retreat",
        "Spacious Seattle Suite", "Historic Boston Home", "Vegas Strip High-Rise",
        "Trendy Austin Bungalow", "Classic Atlanta Townhouse", "Mountain-View Denver Cabin",
        "Orlando Family Villa", "Country Music Stay - Nashville", "Eco-Friendly Portland Flat"
    ],
    "Location": [
        "New York, NY, USA", "Los Angeles, CA, USA", "Chicago, IL, USA", "Miami, FL, USA",
        "Dallas, TX, USA", "San Francisco, CA, USA", "Seattle, WA, USA", "Boston, MA, USA",
        "Las Vegas, NV, USA", "Austin, TX, USA", "Atlanta, GA, USA", "Denver, CO, USA",
        "Orlando, FL, USA", "Nashville, TN, USA", "Portland, OR, USA"
    ],
    "Availability": [
        "2025-05-10", "2025-05-12", "2025-05-14", "2025-05-11", "2025-05-15",
        "2025-05-16", "2025-05-13", "2025-05-18", "2025-05-17", "2025-05-19",
        "2025-05-20", "2025-05-21", "2025-05-22", "2025-05-23", "2025-05-24"
    ],
    "No_of_Guests": [2, 4, 3, 2, 1, 2, 5, 2, 6, 3, 4, 2, 5, 2, 1],
    "Rating": [4.5, 4.8, 4.2, 4.0, 3.8, 4.7, 4.3, 4.6, 4.9, 4.1, 3.9, 4.4, 4.2, 4.0, 3.7],
    "Instant_Booking": [True, False, True, True, False, True, True, False, True, False, True, False, True, True, False],
    "House_Type": [
        "Apartment", "Home", "Apartment", "Beach House", "Studio", "Retreat",
        "Suite", "Home", "High-Rise", "Bungalow", "Townhouse", "Cabin",
        "Villa", "Hotel Room", "Flat"
    ],
    "Price_per_Night": [120, 450, 130, 300, 95, 350, 180, 160, 250, 140, 190, 210, 275, 160, 110],
    "Beds": [1, 3, 2, 2, 1, 2, 3, 2, 3, 2, 3, 2, 4, 1, 1],
    "Rooms": [2, 5, 3, 3, 1, 3, 4, 3, 4, 3, 4, 3, 5, 1, 1],
    "Bathrooms": [1, 3, 2, 2, 1, 2, 2, 1, 3, 2, 2, 1, 3, 1, 1],
    "Amenities": [
        ["WiFi", "Kitchen"], ["Pool", "WiFi", "Parking"], ["WiFi", "AC"], ["Beach Access", "WiFi"],
        ["WiFi"], ["WiFi", "Balcony"], ["Parking", "Kitchen"], ["WiFi", "Washer"], ["Gym", "WiFi"],
        ["WiFi"], ["Parking"], ["Mountain View", "WiFi"], ["WiFi", "Kids Friendly"], ["WiFi"], ["Eco-Friendly"]
    ],
    "Safety_Amenities": [
        ["Smoke Detector"], ["Smoke Detector", "Fire Extinguisher"], ["First Aid Kit"], ["Smoke Detector"],
        ["Fire Extinguisher"], ["Smoke Detector"], ["Smoke Detector"], ["None"], ["First Aid Kit"],
        ["None"], ["None"], ["Fire Extinguisher"], ["Smoke Detector"], ["None"], ["None"]
    ],
    "Shared_Space": [
        False, False, True, False, True, False, True, True, False, True, False, True, False, True, True
    ],
    "House_Rules": [
        ["No smoking"], ["No pets", "No parties"], ["No parties"], ["Quiet hours after 10pm"],
        ["No smoking"], ["No loud music"], ["No parties"], ["No smoking"], ["No pets"],
        ["Quiet hours"], ["No parties"], ["No guests"], ["No pets"], ["No parties"], ["No smoking"]
    ]
}
df = pd.DataFrame(data)
df["Availability"] = pd.to_datetime(df["Availability"])

# Define the LLM class

class YiLLM():
    def __init__(self,prompt):
        self.prompt = prompt

    def call(self):
        try:
            response = requests.post(
                "http://localhost:1234/v1/chat/completions",
                headers={"Content-Type": "application/json"},
                json={
                    "model": "lmstudio-community/Yi-Coder-9B-Chat-GGUF",  # Make sure this matches your loaded model name
                    "messages": [{"role": "user", "content": self.prompt}],
                    "max_tokens": 150,
                    "temperature": 0.1,
                    "top_p": 1.0,
                    "repeat_penalty": 1.1
                },
            )

            # Extract and return just the message content
            output = response.json()["choices"][0]["message"]["content"]
            
            return output
        
        except Exception as e:
            return f"Error {e}"
        

class YiLLM_Eval():
    def __init__(self,prompt):
        self.prompt = prompt

    def call(self):
        try:
            response = requests.post(
                "http://localhost:1234/v1/chat/completions",
                headers={"Content-Type": "application/json"},
                json={
                    "model": "lmstudio-community/Phi-3.1-mini-128k-instruct-GGUF",  # Make sure this matches your loaded model name
                    "messages": [{"role": "user", "content": self.prompt}],
                    "max_tokens": 150,
                    "temperature": 0.1,
                    "top_p": 1.0,
                    "repeat_penalty": 1.1
                },
            )

            # Extract and return just the message content
            output = response.json()["choices"][0]["message"]["content"]
            
            return output
        
        except Exception as e:
            return f"Error {e}"


# Streamlit app layout
st.title("Glyph FEAT-AI")

# Display the DataFrame
st.subheader("Original DataFrame:")
st.write(df)

# Get user input for filtering
user_query = st.text_input("Enter a query to filter the listings (e.g., Rating above 4)")

if user_query:

# Prepare the prompt
    prompt = f"""
    ### Instruction:
    You are a Python assistant that converts user queries into pandas DataFrame filter code.

    Assume the DataFrame `df` has the following columns:
    - Listing_Name (str)
    - Location (str)
    - Availability (datetime)
    - No_of_Guests (int)
    - Rating (float)
    - Instant_Booking (bool)
    - House_Type (str)
    - Price_per_Night (int)
    - Beds (int)
    - Rooms (int)
    - Bathrooms (int)
    - Amenities (list[str])
    - Safety_Amenities (list[str])
    - Shared_Space (bool)
    - House_Rules (list[str])

    ### Formatting & Logic Rules:
    - Use format `YYYY-MM-DD` for dates
    - For date ranges: `start_date to end_date`
    - Booleans as `true` or `false`
    - Lists as Python list format (e.g., ["WiFi", "Pool"])
    - Omit fields not mentioned in user query

    ### Matching Logic:
    - For **all string-based columns** (e.g., Location, Listing_Name, House_Type, House_Rules):
    - Use **case-insensitive partial matching** with `.str.contains("value", case=False)`
    - Avoid exact string comparisons (`==`) unless the query clearly specifies a full value
    - For **list-type fields** (e.g., Amenities, Safety_Amenities, House_Rules):
    - Use `.apply(lambda x: "value" in x)` or match multiple items using `all(...)`/`any(...)` logic
    - For **numerical ranges**, use `>=`, `<=`, or `between`
    - For **date ranges**, use: `(df["Availability"] >= "YYYY-MM-DD") & (df["Availability"] <= "YYYY-MM-DD")`

    ### Date Interpretation Rules:
    - "This weekend": Saturday to Sunday → "{saturday.isoformat()} to {sunday.isoformat()}"
    - "Next weekend": One week later → "{(saturday + timedelta(days=7)).isoformat()} to {(sunday + timedelta(days=7)).isoformat()}"
    - "Tomorrow": Use {tomorrow.isoformat()}
    - If only duration is given, assume start date is today → {today.isoformat()}
    - Always infer `start_date` and `end_date` where relevant

    ### Examples:
    - "in New York" → `df["Location"].str.contains("New York", case=False)`
    - "with pool and WiFi" → `df["Amenities"].apply(lambda x: all(a in x for a in ["pool", "WiFi"]))`
    - "instant book" → `df["Instant_Booking"] == True`
    - "at least 4.5 rating" → `df["Rating"] >= 4.5`
    - Combine conditions with `&` and wrap with `df[...]`
    ---

    ### User Query:
    {user_query}

    ---

    ### Output:
    Only output the final pandas filter expression, using proper logical chaining with `&` or `|` as needed.
    """

    model = YiLLM(prompt)
    # Show the generated pandas code
    st.markdown("### Processing for Code Generation...")
    code_value = model.call()

    st.markdown("### Code Generation Completed, Working on Evaluation...")

    #Evaluation Part
    validation_prompt = f"""
    You are a Python expert assistant. Your task is to validate and correct the given pandas DataFrame filter expression.

    Instructions:
    - Ensure the code is a syntactically valid filter wrapped in `df[...]`.
    - Fix only syntax issues such as:
    - Missing or unmatched brackets.
    - Missing or misplaced logical operators (&, |).
    - Expressions not wrapped with `df[...]`.
    - Do NOT change the logical meaning or alter column names, filters, or values.

    Input Expression:
    {code_value}

    Output:
    Return only the corrected Python expression (wrapped in `df[...]`), nothing else.
    """

    phi_model = YiLLM_Eval(validation_prompt)
    phi_code = phi_model.call()
    # final_code = sanitize_model_code(code_value)

    st.code(phi_code,language="python")
    try:
        # Execute the generated code on the dataframe
        filtered_df = eval(phi_code)
        st.subheader("Filtered Listings:")
        st.write(filtered_df)
    except Exception as e:
        st.error(f"Error executing code: {e}")