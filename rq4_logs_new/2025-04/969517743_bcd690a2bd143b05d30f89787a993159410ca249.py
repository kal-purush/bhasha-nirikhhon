import streamlit as st 

st.title("🌐 Unit Converter App")
st.markdown("### 🛠️ Build your Unit Converter")
st.write("👋 Welcome! Select a category, enter the value, and get the converted result in real-time! ⚙️")

# 📂 Category selector
category = st.selectbox("📂 Select Category:", ["📏 Length", "⚖️ Weight", "⏰ Time", "🌡️ Temperature"])

# 🔄 Conversion logic
def convert(category, value, unit):
    if category == "📏 Length":
        if unit == "📍 Kilometers to Miles":
            return value * 0.621371
        elif unit == "🛣️ Miles to Kilometers":
            return value / 0.621371

    elif category == "⚖️ Weight":
        if unit == "🏋️ Kilograms to Pounds":
            return value * 2.20462
        elif unit == "🪶 Pounds to Kilograms":
            return value / 2.20462

    elif category == "⏰ Time":
        if unit == "⏱️ Seconds to Minutes":
            return value / 60
        elif unit == "⏳ Minutes to Seconds":
            return value * 60
        elif unit == "🕰️ Minutes to Hours":
            return value / 60
        elif unit == "⏱️ Hours to Minutes":
            return value * 60
        elif unit == "🌙 Hours to Days":
            return value / 24 
        elif unit == "🌞 Days to Hours":
            return value * 24

    elif category == "🌡️ Temperature":
        if unit == "🌡️ Celsius to Fahrenheit":
            return (value * 9/5) + 32
        elif unit == "🌡️ Fahrenheit to Celsius":
            return (value - 32) * 5/9
        elif unit == "🌡️ Celsius to Kelvin":
            return value + 273.15
        elif unit == "🌡️ Kelvin to Celsius":
            return value - 273.15
        elif unit == "🌡️ Fahrenheit to Kelvin":
            return (value - 32) * 5/9 + 273.15
        elif unit == "🌡️ Kelvin to Fahrenheit":
            return (value - 273.15) * 9/5 + 32

# 🎛️ Unit selector
if category == "📏 Length":
    unit = st.selectbox("🔁 Select Conversion:", ["📍 Kilometers to Miles", "🛣️ Miles to Kilometers"])

elif category == "⚖️ Weight":
    unit = st.selectbox("🔁 Select Conversion:", ["🏋️ Kilograms to Pounds", "🪶 Pounds to Kilograms"])

elif category == "⏰ Time":
    unit = st.selectbox("🔁 Select Conversion:", [
        "⏱️ Seconds to Minutes", "⏳ Minutes to Seconds",
        "🕰️ Minutes to Hours", "⏱️ Hours to Minutes",
        "🌙 Hours to Days", "🌞 Days to Hours"
    ])

elif category == "🌡️ Temperature":
    unit = st.selectbox("🔁 Select Conversion:", [
        "🌡️ Celsius to Fahrenheit", "🌡️ Fahrenheit to Celsius",
        "🌡️ Celsius to Kelvin", "🌡️ Kelvin to Celsius",
        "🌡️ Fahrenheit to Kelvin", "🌡️ Kelvin to Fahrenheit"
    ])

# 🔢 Input field
value = st.number_input("🔢 Enter the value to convert:")

# 🧮 Convert Button
if st.button("🚀 Convert"):
    if value == 0 and category != "🌡️ Temperature":
        st.warning("⚠️ Please enter a value greater than 0.")
    else:
        result = convert(category, value, unit)
        if result is not None:
            st.success(f"✅ The result is: **{result:.2f}**")
        else:
            st.error("❌ Conversion not available for the selected options.")