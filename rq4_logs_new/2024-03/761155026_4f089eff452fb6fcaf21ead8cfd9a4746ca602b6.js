// RankScraper.js

// Import any necessary libraries or modules
const puppeteer = require("puppeteer");

// Define a function to scrape player rank
async function scrapePlayerRank(playerName) {
  // Launch a headless browser
  const browser = await puppeteer.launch();

  try {
    // Open a new page
    const page = await browser.newPage();

    // Navigate to the Fortnite Tracker website
    await page.goto(
      `https://fortnitetracker.com/profile/all/${encodeURIComponent(
        playerName
      )}`
    );

    // Wait for the player rank element to be present
    await page.waitForSelector(".trn-defstat__value");

    // Extract the player rank
    const rankElement = await page.$(".trn-defstat__value");
    const rankText = await page.evaluate(
      (element) => element.textContent,
      rankElement
    );

    return rankText.trim();
  } catch (error) {
    console.error("Error scrapig player rank:", error);
    return null;
  } finally {
    // Close the browser
    await browser.close();
  }
}

// Export the function to make it accessible from other modules
module.exports = {
  scrapePlayerRank,
};