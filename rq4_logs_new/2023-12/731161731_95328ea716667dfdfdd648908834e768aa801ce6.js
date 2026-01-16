// Function to update table data
function updateTable() {
    // Get form values
    const date = document.getElementById('date').value;
    const panmandi = document.getElementById('panmandi').value;
    const itemName = document.getElementById('itemName').value;
    const quantity = document.getElementById('quantity').value;
    const rate = parseFloat(document.getElementById('rate').value);
  
    // Create or get the data from local storage
    let data = JSON.parse(localStorage.getItem('panMandiData')) || {};
  
    // Create or get the table for the pan mandi
    if (!data[panmandi]) {
      data[panmandi] = [];
    }
  
    // Check if the row already exists based on date and itemName
    const existingRow = data[panmandi].find(row => row.date === date && row.itemName === itemName);
  
    if (existingRow) {
      // If the row exists, update the values
      existingRow.quantity = quantity;
      existingRow.rate = rate;
    } else {
      // If the row doesn't exist, add a new row
      data[panmandi].unshift({
        date,
        itemName,
        quantity,
        rate
      });
    }
  
    // Update local storage
    localStorage.setItem('panMandiData', JSON.stringify(data));
  
    // Render tables
    renderTables(data);
  
    // Clear the form after adding the row
    document.getElementById('dataForm').reset();
  }
  
  // Function to render tables
  function renderTables(data) {
    const tablesContainer = document.getElementById('tablesContainer');
    tablesContainer.innerHTML = '';
  
    for (const panmandi in data) {
      const table = document.createElement('table');
      table.innerHTML = `
        <caption>${panmandi} Table</caption>
        <thead>
          <tr>
            <th>Date</th>
            <th>Item Name</th>
            <th>Quantity</th>
            <th>Rate</th>
            <th>Total</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody></tbody>
        <tfoot>
          <tr>
            <td colspan="4">Monthly Total:</td>
            <td id="${panmandi}MonthlyTotal">0</td>
            <td></td>
          </tr>
        </tfoot>
      `;
  
      const tableBody = table.getElementsByTagName('tbody')[0];
      for (const row of data[panmandi]) {
        const newRow = tableBody.insertRow(0);
        const dateCell = newRow.insertCell(0);
        const itemNameCell = newRow.insertCell(1);
        const quantityCell = newRow.insertCell(2);
        const rateCell = newRow.insertCell(3);
        const totalCell = newRow.insertCell(4);
        const actionCell = newRow.insertCell(5);
  
        dateCell.innerHTML = row.date;
        itemNameCell.innerHTML = row.itemName;
        quantityCell.innerHTML = row.quantity;
        rateCell.innerHTML = row.rate.toFixed(2);
        totalCell.innerHTML = (row.quantity * row.rate).toFixed(2);
  
        const editButton = document.createElement('button');
        editButton.textContent = 'Edit';
        editButton.addEventListener('click', () => editRow(panmandi, row));
  
        const deleteButton = document.createElement('button');
        deleteButton.textContent = 'Delete';
        deleteButton.addEventListener('click', () => deleteRow(panmandi, row));
  
        actionCell.appendChild(editButton);
        actionCell.appendChild(deleteButton);
      }
  
      tablesContainer.appendChild(table);
      updateMonthlyTotal(panmandi);
    }
  }
  
  // Function to edit a row
  function editRow(panmandi, row) {
    // Populate the form with the values of the selected row
    document.getElementById('date').value = row.date;
    document.getElementById('panmandi').value = panmandi;
    document.getElementById('itemName').value = row.itemName;
    document.getElementById('quantity').value = row.quantity;
    document.getElementById('rate').value = row.rate;
  
    // Change the button to an 'Update' button
    const addButton = document.querySelector('button');
    addButton.textContent = 'Update';
    addButton.onclick = function () {
      updateTable(); // Call the updateTable function for editing
      // Reset the form and revert the button text and click event
      document.getElementById('dataForm').reset();
      addButton.textContent = 'Add Row';
      addButton.onclick = updateTable;
    };
  }
  
  // Function to delete a row
  function deleteRow(panmandi, row) {
    // Get the data from local storage
    let data = JSON.parse(localStorage.getItem('panMandiData')) || {};
  
    // Find and remove the row based on date and itemName
    data[panmandi] = data[panmandi].filter(r => r.date !== row.date || r.itemName !== row.itemName);
  
    // Update local storage
    localStorage.setItem('panMandiData', JSON.stringify(data));
  
    // Re-render tables
    renderTables(data);
  }
  
  // Function to update the monthly total
  function updateMonthlyTotal(panmandi) {
    const data = JSON.parse(localStorage.getItem('panMandiData')) || {};
    const monthlyTotal = data[panmandi].reduce((total, row) => total + row.quantity * row.rate, 0);
    document.getElementById(`${panmandi}MonthlyTotal`).textContent = monthlyTotal.toFixed(2);
  }
  
  // Function to manually withdraw the total amount and start a new table
  function withdrawManualTotal() {
    const panmandi = document.getElementById('panmandi').value;
    const data = JSON.parse(localStorage.getItem('panMandiData')) || {};
    const withdrawalAmount = parseFloat(document.getElementById('withdrawalAmount').value) || 0;
  
    // Calculate the remaining total after withdrawal
    const remainingTotal = data[panmandi].reduce((total, row) => total + row.quantity * row.rate, 0) - withdrawalAmount;
  
    // Create a new table with the remaining data
    const newTable = document.createElement('table');
    newTable.innerHTML = `
      <caption>${panmandi} Table (New)</caption>
      <thead>
        <tr>
          <th>Date</th>
          <th>Item Name</th>
          <th>Quantity</th>
          <th>Rate</th>
          <th>Total</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
      <tfoot>
        <tr>
          <td colspan="4">Monthly Total:</td>
          <td id="${panmandi}MonthlyTotal">${remainingTotal.toFixed(2)}</td>
          <td></td>
        </tr>
      </tfoot>
    `;
  
    const newTableBody = newTable.getElementsByTagName('tbody')[0];
    for (const row of data[panmandi]) {
      const newRow = newTableBody.insertRow(0);
      const dateCell = newRow.insertCell(0);
      const itemNameCell = newRow.insertCell(1);
      const quantityCell = newRow.insertCell(2);
      const rateCell = newRow.insertCell(3);
      const totalCell = newRow.insertCell(4);
      const actionCell = newRow.insertCell(5);
  
      dateCell.innerHTML = row.date;
      itemNameCell.innerHTML = row.itemName;
      quantityCell.innerHTML = row.quantity;
      rateCell.innerHTML = row.rate.toFixed(2);
      totalCell.innerHTML = (row.quantity * row.rate).toFixed(2);
  
      const editButton = document.createElement('button');
      editButton.textContent = 'Edit';
      editButton.addEventListener('click', () => editRow(panmandi, row));
  
      const deleteButton = document.createElement('button');
      deleteButton.textContent = 'Delete';
      deleteButton.addEventListener('click', () => deleteRow(panmandi, row));
  
      actionCell.appendChild(editButton);
      actionCell.appendChild(deleteButton);
    }
  
    // Append the new table to the container
    document.getElementById('tablesContainer').appendChild(newTable);
  
    // Reset the monthly total and update the display
    document.getElementById(`${panmandi}MonthlyTotal`).textContent = remainingTotal.toFixed(2);
  
    // Display breaking news with the last withdrawal date
    const breakingNewsContainer = document.getElementById('breakingNewsContainer');
    breakingNewsContainer.innerHTML = `<p>Last Withdrawal Date (${panmandi}): ${new Date().toLocaleDateString()}</p>`;
  }
  
  // Initialize tables on page load
  const initialData = JSON.parse(localStorage.getItem('panMandiData')) || {};
  renderTables(initialData);
  
  // Function to print the page
  function printPage() {
    window.print();
  }
  