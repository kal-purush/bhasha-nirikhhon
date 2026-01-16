let submitButton = document.getElementById("submit-button");
let countryNameInput = document.getElementById("country-name-input");
let countryInformation = document.getElementById("country-info");
let borderingCountries = document.getElementById("bordering-countries");

submitButton.addEventListener("click", function() {
  processJSON(countryNameInput.value.trim());
});

async function processJSON(countryName) {
  const url = `https://restcountries.com/v3.1/name/${countryName}`;

  try {
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error("Error: can't find country");
    }

    const countryDataJSON = await response.json();
    const country = countryDataJSON[0];
    const imageUrl = country.flags.png;



    countryInformation.innerHTML = `
      <p>${country.name.common}</p>
      <p>Capital: ${country.capital ? country.capital[0] : "n/a"}</p>
      <p>Population: ${country.population}</p>
      <p>Region: ${country.region}</p>
      <p>Flag: </p>
      <img src="${imageUrl}" > 
    `;

    if (country.borders) {

      let bordersHtml = `<p>Bordering Countries:</p>`;

      for (let border of country.borders) {
        const borderUrl = `https://restcountries.com/v3.1/alpha/${border}`;
        const borderResponse = await fetch(borderUrl);

        if (!borderResponse.ok) {
          continue; 
        }

        const borderCountryData = await borderResponse.json();
        const borderCountry = borderCountryData[0];
        const borderImageUrl = borderCountry.flags.png;

        bordersHtml += `
            <p>${borderCountry.name.common}:</p>
            <img src="${borderImageUrl}">
        `;
      }
      borderingCountries.innerHTML = bordersHtml;
    }

  } catch (error) {
    console.error(error.message);
  }

}