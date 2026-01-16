describe("Climate Class", () => {
  it("should fetch weather data successfully", () => {
    // Mock successful API response
    cy.intercept(
      "GET",
      "https://api.openweathermap.org/data/2.5/weather?q=London&appid=YOUR_API_KEY",
      {
        body: {
          // mocked data
          main: {
            temp: 280,
          },
          weather: [
            {
              description: "cloudy",
            },
          ],
        },
      }
    ).as("getWeather");

    // Call your Climate.getWeather() method directly
    cy.window()
      .invoke("Climate.getWeather", "London")
      .then((data: any) => {
        expect(data.main.temp).to.eq(280);
        expect(data.weather[0].description).to.eq("cloudy");
      });
  });

  it("should handle errors gracefully", () => {
    // Mock failed API response
    cy.intercept(
      "GET",
      "https://api.openweathermap.org/data/2.5/weather?q=InvalidCity&appid=YOUR_API_KEY",
      {
        statusCode: 404,
        body: {
          cod: "404",
          message: "city not found",
        },
      }
    ).as("getWeather");

    cy.window()
      .invoke("Climate.getWeather", "InvalidCity")
      .then((data: any) => {
        // Note: Here you'd be checking the behavior of your service in case of an error.
        // This depends on how your service handles errors.
        // For this example, I'm assuming you'd have thrown an error.
        expect(data).to.have.property(
          "message",
          "Error fetching weather data:"
        );
      });
  });
});