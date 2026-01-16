export default function Weather({ weather }) {
  const weatherMarkup = weather.map((day) => {
    return (
      <div className="day" key={day.date}>
        <h3>
          <span>{day.date}: </span>
          <span>{day.description}</span>
        </h3>
      </div>
    );
  });

  return (
    <section>
      <h2>Weather for the next 3 days:</h2>
      {weatherMarkup}
    </section>
  );
}