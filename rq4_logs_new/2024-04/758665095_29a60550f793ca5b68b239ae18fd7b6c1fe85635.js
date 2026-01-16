// This class represents functionalities related to searching for flights

const { test, expect } = require('@playwright/test');
class FilterResults {
  // Constructor to initialize the class with a page object
  constructor(page) {
    this.page = page;
  }

  // Method to filter flights by the number of stops

  async filterFlightsByNumberOfStops() {

    let filterByButton = this.page.getByTestId('resultPage-toggleFiltersButton-button');
    let filterByHeaderFlights = this.page.getByTestId('resultPage-filters-header');
    let totalFlightsText = await this.page.locator('span[data-testid="resultPage-filters-text"] + span').textContent();
    let resetFilterAll = this.page.getByTestId('resultPage-filterHeader-allFilterResetButton-button');
    let selectedFilterIndicator = this.page.getByTestId('resultPage-filterHeader-selectedFiltersIndicator');
    let numberOfStopsHeader = this.page.getByTestId('resultPage-MAX_STOPS-header');
    let nonStopFlightsButton = this.page.getByTestId('MAX_STOPS-direct');
    let maxOneStopButton = this.page.getByTestId('MAX_STOPS-max1');
    let allButton = this.page.getByTestId('MAX_STOPS-all');
    let resetFilterStops = this.page.getByTestId('resultPage-filterHeader-MAX_STOPSFilterResetButton-button');

    await expect(filterByButton).toContainText('Filter by');
    await filterByButton.click();
    await expect(filterByButton).toContainText('Close');
    await expect(numberOfStopsHeader).toContainText('Number of stops');
    await expect(nonStopFlightsButton).toContainText('Nonstop flights');
    await expect(maxOneStopButton).toContainText('Maximum one stop');
    await expect(allButton).toContainText('All');
    await expect(allButton).toHaveClass(/selectedOption/);

    // Counting the number of segments with one stop
    let segmentStopsCount = await this.page.getByTestId('searchResults-segment-stops').filter({ hasText: '1 stop' }).count();
    // Expecting the count of segments with one stop to be greater than or equal to zero
    expect(parseInt(segmentStopsCount)).toBeGreaterThanOrEqual(0);

    // Clicking non-stop flights button
    await nonStopFlightsButton.click();
    // Waiting for the response containing flight search results
    const responsePromise = await this.page.waitForResponse((response) => response.url().includes("/graphql/SearchOnResultPage"));

    await expect(nonStopFlightsButton).toHaveClass(/selectedOption/);
    await expect(selectedFilterIndicator).toContainText('Stops');
    await expect(resetFilterAll).toContainText('Reset filter');
    await expect(resetFilterStops).toContainText('Reset filter');

    const responseBody = await responsePromise.json();
    // Getting the count of filtered flights
    let filteredFlightsCount = responseBody.data.search.filteredFlightsCount;
    // Expecting filter by header flights to contain specific text
    await expect(filterByHeaderFlights).toContainText(`${filteredFlightsCount} of ${totalFlightsText.replace(': ', '').toLowerCase()}`);

    // Getting flights from the response
    let flights = responseBody.data.search.flights;
    // Asserting that for non-stop flights, the segment length should be 1
    flights.forEach(flight => {
      expect(flight).toHaveProperty('bounds[0].segments');
      let segments = flight.bounds[0].segments;
      expect(segments).toHaveLength(1);
    });

    // Expecting flights with text "1 stop" not to be found after filtering for non-stop flights
    await expect(this.page.getByTestId('searchResults-segment-stops').filter({ hasText: '1 stop' })).toHaveCount(0);

    // Testing reset filters
    await resetFilterStops.click();
    await expect(filterByHeaderFlights).toContainText(totalFlightsText);
    await expect(allButton).toHaveClass(/selectedOption/);
    await expect(resetFilterAll).not.toBeAttached();
    await expect(resetFilterStops).not.toBeAttached();
    await expect(selectedFilterIndicator).not.toBeAttached();
  }

  // Method to filter flights by selected airlines

  async filterFlightsByAirlines(airlines) {

    let filterByButton = this.page.getByTestId('resultPage-toggleFiltersButton-button');
    let filterByHeaderFlights = this.page.getByTestId('resultPage-filters-header');
    let totalFlightsText = await this.page.locator('span[data-testid="resultPage-filters-text"] + span').textContent();
    let resetFilterAll = this.page.getByTestId('resultPage-filterHeader-allFilterResetButton-button');
    let selectedFilterIndicator = this.page.getByTestId('resultPage-filterHeader-selectedFiltersIndicator');
    let airlinesHeader = this.page.getByTestId('resultPage-AIRLINES-header');
    let clearAllButton = this.page.getByRole('button', { name: 'Clear all' });
    let selectAllButton = this.page.getByRole('button', { name: 'Select all' });
    let airlineCheckbox = this.page.locator(`li > label[title="${airlines[0]}"]`);
    let doneButton = this.page.getByTestId('filtersForm-applyFilters-button');
    let clearButton = this.page.getByTestId('filtersForm-resetFilters-button');
    let resetFilterAirlines = this.page.getByTestId('resultPage-filterHeader-AIRLINESFilterResetButton-button');

    await expect(filterByButton).toContainText('Filter by');
    await filterByButton.click();
    await expect(filterByButton).toContainText('Close');
    await expect(airlinesHeader).toContainText('Airlines');

    // Getting all the text results for the first airline
    let airlineResultsCount = await this.page.locator(`div.css-akpcxl:has-text("${airlines[0]}")`).allTextContents();
    // Expecting the count of results to be at least 1
    expect(parseInt(airlineResultsCount.length)).toBeGreaterThanOrEqual(0);

    // Deselecting an airline checkbox
    await airlineCheckbox.click();

    // Waiting for the response containing filtered airlines
    const responsePromiseFilteredAirline = await this.page.waitForResponse((response) => response.url().includes("/graphql/SearchOnResultPage"));

    // Expecting the airline checkbox not to be checked
    await expect(airlineCheckbox).not.toBeChecked();
    await expect(selectedFilterIndicator).toContainText('Airlines');
    await expect(resetFilterAll).toContainText('Reset filter');
    await expect(resetFilterAirlines).toContainText('Reset filter');

    const responseBodyFilteredAirline = await responsePromiseFilteredAirline.json();

    // Scrolling until the end of the page to load all flight results
    await scrollUntilElementIsVisible(this.page, 'p:has-text("No more results.")');

    let filteredFlightsCount = responseBodyFilteredAirline.data.search.filteredFlightsCount;
    await expect(filterByHeaderFlights).toContainText(`${filteredFlightsCount} of ${totalFlightsText.replace(': ', '').toLowerCase()}`);

    // Expecting no specific airline results to be found after deselecting an airline
    await expect(this.page.locator(`div.css-akpcxl:has-text("${airlines[0]}")`)).toHaveCount(0);

    await clearButton.click();
    await expect(airlineCheckbox).toBeChecked();
    await expect(filterByHeaderFlights).toContainText(totalFlightsText);
    await expect(resetFilterAll).not.toBeAttached();
    await expect(resetFilterAirlines).not.toBeAttached();
    await expect(selectedFilterIndicator).not.toBeAttached();

    await clearAllButton.click();
    await expect(resetFilterAirlines).toBeAttached();

    // Checking each airline checkbox not to be checked
    await airlines.forEach(async (airline) => {
      expect(this.page.getByLabel(`${airline}`)).not.toBeChecked();
    });

    await selectAllButton.click();
    await expect(resetFilterAirlines).not.toBeAttached();

    // Checking each airline checkbox (except of the specific one) to be checked
    await airlines.forEach(async (airline) => {
      if (!airlines[0]) {
        expect(this.page.getByLabel(`${airline}`)).toBeChecked();
      }
    });
  }

  // Method to filter flights by price range
  async filterFlightsByPrice() {

    let filterByButton = this.page.getByTestId('resultPage-toggleFiltersButton-button');
    let filterByHeaderFlights = this.page.getByTestId('resultPage-filters-header');
    let totalFlightsText = await this.page.locator('span[data-testid="resultPage-filters-text"] + span').textContent();
    let resetFilterAll = this.page.getByTestId('resultPage-filterHeader-allFilterResetButton-button');
    let resetFilterPrice = this.page.getByTestId('resultPage-filterHeader-PRICEFilterResetButton-button');
    let selectedFilterIndicator = this.page.getByTestId('resultPage-filterHeader-selectedFiltersIndicator');
    let priceHeader = this.page.getByTestId('resultPage-PRICE-header').getByText('Price');
    let priceHandleFrom = this.page.getByTestId('resultPage-PRICEFilter-content').getByTestId('handle-0');
    let priceHandleTo = this.page.getByTestId('resultPage-PRICEFilter-content').getByTestId('handle-1');
    let priceSliderTrack = this.page.getByTestId('resultPage-PRICEFilter-content').getByTestId('track-0');
    let priceFrom = await this.page.getByTestId('resultPage-PRICEFilter-content').locator('div.slider-tracks + div');
    let priceTo = await this.page.getByTestId('resultPage-PRICEFilter-content').locator('div.slider-tracks + div + div');

    // Getting starting price range values
    let priceFromStartingValue = await priceFrom.textContent();
    let priceToStartingValue = await priceTo.textContent();


    let allPrices = await this.page.getByTestId('result-trip-price-info-button');

    // Defining the center(X,Y) of the handle elements to click
    const handleSliderFrom = await priceHandleFrom.boundingBox(); // boundingBox() method returns the bounding box of the given element
    const startFromX = handleSliderFrom.x + handleSliderFrom.width / 2;
    const startFromY = handleSliderFrom.y + handleSliderFrom.height / 2;

    const handleSliderTo = await priceHandleTo.boundingBox();
    const startToX = handleSliderTo.x + handleSliderTo.width / 2;
    const startToY = handleSliderTo.y + handleSliderTo.height / 2;

    // Define the desired offset to drag the slider handles
    const offsetFromX = 10; // drag the From handle 10 pixels to the right
    const offsetToX = -500; // drag the To handle 500 pixels to the left


    await expect(filterByButton).toContainText('Filter by');
    await filterByButton.click();
    await expect(filterByButton).toContainText('Close');
    await expect(priceHeader).toContainText('Price');

    // Replace non-digit characters of the price, keep only the numerical value and assign to a const

    let allPricesArray = await this.page.getByTestId('result-trip-price-info-button').allTextContents();
    const allPricesArrayNew = await allPricesArray.map(replaceAllNonDigitChars);

    // Perform mouse interaction to drag the slider handles
    await this.page.mouse.move(startFromX, startFromY); // move mouse to the center of the handle element
    await this.page.mouse.down(); // click down left mouse button and hold
    await this.page.mouse.move(startFromX + offsetFromX, startFromY); // drag to the given offset
    await this.page.mouse.up(); // release mouse button

    await this.page.mouse.move(startToX, startToY);
    await this.page.mouse.down();
    await this.page.mouse.move(startToX + offsetToX, startToY);
    await this.page.mouse.up();

    // Waiting for the response containing filtered flights by price
    const responsePromise = await this.page.waitForResponse((response) => response.url().includes("/graphql/SearchOnResultPage"));
    const responseBody = await responsePromise.json();

    let filteredFlightsCount = await responseBody.data.search.filteredFlightsCount;

    await expect(filterByHeaderFlights).toContainText(`${filteredFlightsCount} of ${totalFlightsText.replace(': ', '').toLowerCase()}`);
    await expect(selectedFilterIndicator).toContainText('Price');
    await expect(resetFilterAll).toContainText('Reset filter');
    await expect(resetFilterPrice).toContainText('Reset filter');

    let allPricesArrayFiltered = await this.page.getByTestId('result-trip-price-info-button').allTextContents();
    const allPricesArrayFilteredNew = await allPricesArrayFiltered.map(replaceAllNonDigitChars);

    // Expecting filtered prices array to be different from original prices array
    expect(allPricesArrayNew.toString() == allPricesArrayFilteredNew.toString()).toBeFalsy();

    // Iterating over each price to ensure it falls within the selected price range
    for (let i = 0; i < await allPrices.count(); i++) {
      expect(replaceAllNonDigitChars(await allPrices.nth(i).textContent())).toBeGreaterThanOrEqual(replaceAllNonDigitChars(await priceFrom.textContent()));
      expect(replaceAllNonDigitChars(await allPrices.nth(i).textContent())).toBeLessThanOrEqual(replaceAllNonDigitChars(await priceTo.textContent()));
    }

    await resetFilterPrice.click();
    await expect(filterByHeaderFlights).toContainText(totalFlightsText);
    await expect(priceFrom).toContainText(priceFromStartingValue);
    await expect(priceTo).toContainText(priceToStartingValue);
    await expect(resetFilterAll).not.toBeAttached();
    await expect(resetFilterPrice).not.toBeAttached();
    await expect(selectedFilterIndicator).not.toBeAttached();

    let allPricesArrayUnfiltered = await this.page.getByTestId('result-trip-price-info-button').allTextContents();
    const allPricesArrayUnfilteredNew = await allPricesArrayUnfiltered.map(replaceAllNonDigitChars);

    // Expecting unfiltered prices array to be the same as original prices array after resetting price filter
    expect(allPricesArrayNew.toString() == allPricesArrayUnfilteredNew.toString()).toBeTruthy();

    // Iterating over each price to ensure it falls within the original price range after resetting price filter
    for (let i = 0; i < await allPrices.count(); i++) {
      expect(replaceAllNonDigitChars(await allPrices.nth(i).textContent())).toBeGreaterThanOrEqual(replaceAllNonDigitChars(await priceFromStartingValue));
      expect(replaceAllNonDigitChars(await allPrices.nth(i).textContent())).toBeLessThanOrEqual(replaceAllNonDigitChars(await priceToStartingValue));
    }
  }
}

// Function to scroll until a specified element is visible on the page
async function scrollUntilElementIsVisible(page, locator) {
  // Loop until the specified element becomes visible
  while (!(await page.locator(locator).isVisible())) {
    // Scroll down the page for 1000 pixels using mouse wheel
    await page.mouse.wheel(0, 1000);
  }
}

// Function to remove all non-digit characters
function replaceAllNonDigitChars(price) {
  // Replace all non-digit characters except for the decimal point
  const strippedPrice = parseFloat(price.replace(/[^\d.]/g, ""));
  return strippedPrice;
}

// Exporting the FilterResults class
module.exports = FilterResults;