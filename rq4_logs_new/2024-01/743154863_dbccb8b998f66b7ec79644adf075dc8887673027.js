const { Builder, By, Key, until, Select } = require("selenium-webdriver");
const Papa = require("papaparse");
const fs = require("fs");

async function example() {
  let driver = await new Builder().forBrowser("firefox").build();
  try {
    await driver.get("https://apps.adcogov.org/PTForeclosureSearch/index.aspx");

    try {

    
    const inputValue = "Accept Terms";
    const xpathExpression = `//input[@value='${inputValue}']`;

    await driver.wait(until.elementLocated(By.xpath(xpathExpression)), 5000);
    const inputElement = await driver.findElement(By.xpath(xpathExpression));
    await inputElement.click();

    } catch (e) {
        console.log('Already accepted terms')
    }

    const currentDate = new Date();
    const date240DaysAgo = new Date(currentDate);
    date240DaysAgo.setDate(currentDate.getDate() - 240);
    const formattedDate = `${date240DaysAgo.getMonth() + 1}/${date240DaysAgo.getDate()}/${date240DaysAgo.getFullYear()}`;

    const datepickerInput = await driver.findElement(By.id("ContentPlaceHolder1_txtNedDate1"));
    const dropdownElement = await driver.findElement(By.id("ContentPlaceHolder1_ddStatus"));
    const dropdown = new Select(dropdownElement);

    await dropdown.selectByVisibleText("NED Recorded");
    await datepickerInput.clear();
    await datepickerInput.sendKeys(formattedDate, Key.RETURN);

    const inputValueTwo = "Search";
    const xpathExpressionTwo = `//input[@value='${inputValueTwo}']`;

    await driver.wait(until.elementLocated(By.xpath(xpathExpressionTwo)), 5000);
    const inputElementTwo = await driver.findElement(By.xpath(xpathExpressionTwo));

    try {
      await inputElementTwo.click();
    } catch (e) {}

    await driver.wait(until.elementLocated(By.className("SearchResultsGridRow")), 10000);

    const numRecordsElement = await driver.findElement(By.id("ContentPlaceHolder1_Label1"));
    const numRecordsStr = await numRecordsElement.getText();
    const numRecordsStrArr = numRecordsStr.split(" ");
    const numRecords = parseInt(numRecordsStrArr[3]);
    console.log("Found a total of: ", numRecords, " records");

    let currentPage = 1;
    const allPageData = [];
    while (true) {
      try {
        const pageData = await parsePage(driver, currentPage);
        allPageData.push(pageData);
        currentPage++;
        const nextPageXpath = `//*[@id="ContentPlaceHolder1_gvSearchResults"]/tbody/tr[28]/td/table/tbody/tr/td[${currentPage}]/a`;
        await driver.wait(until.elementLocated(By.xpath(nextPageXpath)), 5000);
        const nextPageElement = await driver.findElement(By.xpath(nextPageXpath));
        await nextPageElement.click();
        await driver.sleep(10000);
      } catch (e) {
        console.log(e);
        console.log("No more pages");
        break;
      }
    }

    const flattenedPageData = allPageData.flat();
    const csv = Papa.unparse(flattenedPageData);
    console.log(csv);
  
    // use FS to create a new file
    fs.writeFileSync("./outputs/adams.csv", csv);
  } finally {
    // await driver.quit();
  }
}

async function parsePage(driver, pageNumber) {
  let SearchResultRows = await driver.findElements(By.className("SearchResultsGridRow"));
  let SearchResultRowsAlt = await driver.findElements(By.className("SearchResultsGridAltRow"));
  SearchResultRows = SearchResultRows.concat(SearchResultRowsAlt);
  let rows = [];

  for (const row of SearchResultRows) {
    const cells = await row.findElements(By.tagName("td"));
    const linkCell = cells[0];
    const ownerNameCell = cells[1];
    const addressCell = cells[2];
    const zipCodeCell = cells[3];

    const link = await linkCell.findElement(By.tagName("a")).getAttribute("href");
    const ownerName = await ownerNameCell.getText();
    const addressLine1 = await addressCell.getText();
    const zipCode = await zipCodeCell.getText();

    let rowData = {
      link,
      ownerName,
      addressLine1,
      zipCode,
    };
    rows.push(rowData);
  }
  console.log('Found ', rows.length, ' records on page ', pageNumber, ' of results')
  return rows;
}

example();