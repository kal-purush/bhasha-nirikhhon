import { checkElement } from "../../main.ts"; // Import function to check the existence of an element

/**
 * Class representing the weather component displaying current weather information.
 */
export default class WeatherComponents {
    private readonly weather: HTMLElement;

    /**
     * Creates an instance of WeatherComponents.
     * @param temp - Temperature as a string.
     * @param station - Name of the weather station.
     * @param number - Station number as a string.
     * @param date - Date of the weather measurement.
     * @param time - Time of the weather measurement.
     */
    constructor(
        private readonly temp: string,
        private readonly station: string,
        private readonly number: string,
        private readonly date: string,
        private readonly time: string
    ) {
        this.weather = checkElement(document.getElementById('weather')); // Get the weather container element
    }

    /**
     * Creates and appends a temperature element to the weather container.
     */
    private createTemp(): void {
        const h1 = document.createElement("h1"); // Create an <h1> element
        this.weather.appendChild(h1); // Append to the weather container
        h1.className = "temp"; // Set class name for styling
        h1.innerText = this.temp + " °C"; // Set the temperature text
    }

    /**
     * Creates and appends a city name element to the weather container.
     */
    private createCity(): void {
        const h2 = document.createElement("h2"); // Create an <h2> element
        this.weather.appendChild(h2); // Append to the weather container
        h2.className = "city"; // Set class name for styling
        h2.innerText = this.station; // Set the city name text
    }

}