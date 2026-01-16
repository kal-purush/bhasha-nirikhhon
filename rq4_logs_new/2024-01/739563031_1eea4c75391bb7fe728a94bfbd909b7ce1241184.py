import pandas as pd
import matplotlib.pyplot as plt

class TemperatureConverter:
    def celsius_to_fahrenheit(self, celsius):
        return celsius * 9/5 + 32

    def celsius_to_kelvin(self, celsius):
        return celsius + 273.15


def main():
    city_df, city_name = get_city_data()
    num_iterations = get_number_of_iterations()

    # Create empty lists and initiate a loop
    dates = []
    avg_temps = []
    precipitations = []

    for _ in range(num_iterations):
        input_date = input("Enter a date (YYYY-MM-DD): ")
        selected_date = city_df[city_df['date'] == input_date]

        if not selected_date.empty:
            avg_temp_c = selected_date['avg_temp_c'].values[0]
            precipitation = selected_date['precipitation_mm'].values[0]

            # Convert temperatures to Farenheit and Kelvin
            temp_converter = TemperatureConverter()
            avg_temp_f = temp_converter.celsius_to_fahrenheit(avg_temp_c)
            avg_temp_k = temp_converter.celsius_to_kelvin(avg_temp_c)

            # Classify the temperatures and precipitations
            temp_classification_result = temp_classification(avg_temp_c)
            prec_classification_result = prec_classification(precipitation)

            print(f'Data for {input_date} in {city_name}:')
            print(f'Average Temperature: {avg_temp_c:.2f}°C, ({avg_temp_f:.2f}°F, {avg_temp_k:.2f}K) {temp_classification_result}')
            print(f'Precipitation: {precipitation} {prec_classification_result}')

            dates.append(input_date)
            avg_temps.append(avg_temp_c)
            precipitations.append(precipitation)

        else:
            print(f"No data found for the date {input_date}")

    plot_weather_trend(dates, avg_temps, precipitations, city_name)


def get_city_data():
    file = 'daily_weather.parquet'
    while True:
        city_name = input("Enter the city name: ").lower().capitalize()

        df = pd.read_parquet(file)

        # Filter data for the specified city
        city_df = df[df['city_name'] == city_name]

        if not city_df.empty:
            return city_df, city_name
        else:
            print(f"No data found for the city {city_name}. Please, try again!")


def get_number_of_iterations():
    while True:
        try:
            num_iterations = int(input("Enter the number of iterations: "))
            if num_iterations > 0:
                return num_iterations
            else:
                print("Please, enter a positive number of iterations.")
        except ValueError:
            print("Invalid input. Please, enter a valid number!")            


def temp_classification(celsius):
    emoji = "" 
    if celsius < 0:
        classification = "Freezing temperatures!"
        emoji = "🥶"
    elif 0 <= celsius <= 10:
        classification = "Cold temperatures"
        emoji = "❄️"
    elif 10 < celsius < 20:
        classification = "Moderate temperatures"
        emoji = "😊"
    elif celsius >= 20:
        classification = "Hot temperatures!"
        emoji = "🥵"
    else:
        classification = "Missing temperature data"    

    return f"{classification} {emoji}"


def prec_classification(precipitation_mm):
    if precipitation_mm == 0.0:
        prec_clas = "No rain ☀️"
    elif 0.0 < precipitation_mm <= 0.2:
        prec_clas = "Light rain ☂️"
    elif 0.2 < precipitation_mm < 7.6:
        prec_clas = "Moderate rain 🌧️"
    elif precipitation_mm >= 7.6:
        prec_clas = "Heavy rain! 🌧️🌧️"
    else:
        prec_clas = "Missing precipitation data"    

    return prec_clas


def plot_weather_trend(dates, avg_temps, precipitations, city_name):
    trend_df = pd.DataFrame({'Date': dates, 'Average Temperature': avg_temps, 'Precipitation': precipitations})

    plt.figure(figsize=(10, 6))
    plt.plot(trend_df['Date'], trend_df['Average Temperature'], label='Average Temperature', marker='o', color='orange')
    plt.plot(trend_df['Date'], trend_df['Precipitation'], label='Precipitation', marker='o', color='blue')
    plt.xlabel('Date')
    plt.ylabel('Value')
    plt.title(f'Temperature and Precipitation Trend in {city_name}')
    plt.legend()
    plt.show()


if __name__ == "__main__":
    main()
