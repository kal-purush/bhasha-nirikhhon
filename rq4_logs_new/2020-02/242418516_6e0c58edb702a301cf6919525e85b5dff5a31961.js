// from data.js
var tableData = data;

// get table references
var tbody = d3.select("tbody");

function buildTable(data) {
  // delete any existing data
  tbody.html("");

  // loop through each object in the data and append a rows and cells
  data.forEach((addRow) => {
    // Append a row to the table body
    var row = tbody.append("tr");

    // Loop through each field in the dataRow and add
    // each value as a table cell (td)
    Object.values(addRow).forEach((val) => {
      var cell = row.append("td");
        cell.text(val);
      }
    );
  });
}

function handleClick() {

  // Get the date from the filter input
  var date = d3.select("#datetime").property("value");
  let filteredData = tableData;

  // Look for date entry into the filter field
  if (date) {
      filteredData = filteredData.filter(row => row.datetime === date);
  }
  // Reload the table using the filtered data
    buildTable(filteredData);
}

// Add an event to listen to clicks on Filter Table button
d3.selectAll("#filter-btn").on("click", handleClick);

// Build the full table when page is reloaded
buildTable(tableData);