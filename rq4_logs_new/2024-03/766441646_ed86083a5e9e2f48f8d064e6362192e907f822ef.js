// Fetching data using .then
function fetchDataThen() {
    fetch('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false')
      .then(response => response.json())
      .then(data => {
        globalData = data;
        renderTable(globalData);
      })
      .catch(error => console.error('Error fetching data:', error));
  }
  
  // Fetching data using async/await
  async function fetchDataAsyncAwait() {
    try {
      const response = await fetch('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false');
      const data = await response.json();
      globalData = data;
      renderTable(globalData);
    } catch (error) {
      console.error('Error fetching data:', error);
    }
  }
  
  // Render the table
  function renderTable(data) {
    const tableBody = document.getElementById('tableBody');
    tableBody.innerHTML = ''; // Clear existing table data
  
    data.forEach(item => {
      const row = `
        <tr>
          <td><img src="${item.image}" alt="${item.name}" class="coin-image"/>${item.name}</td>
          <td>${item.symbol.toUpperCase()}</td>
          <td>$${item.current_price.toLocaleString()}</td>
          <td>${item.total_volume.toLocaleString()}</td>
          <td>${item.price_change_percentage_24h.toFixed(2)}%</td>
          <td>${item.market_cap.toLocaleString()}</td>
        </tr>
      `;
      tableBody.innerHTML += row;
    });
  }
  
  // Search functionality
  function searchData() {
    const searchValue = document.getElementById('searchInput').value.toLowerCase();
    const filteredData = globalData.filter(item => item.name.toLowerCase().includes(searchValue) || item.symbol.toLowerCase().includes(searchValue));
    renderTable(filteredData);
  }
  
  // Sort by market cap
  function sortDataByMarketCap() {
    const sortedData = [...globalData].sort((a, b) => b.market_cap - a.market_cap);
    renderTable(sortedData);
  }
  
  // Sort by percentage change
  function sortDataByPercentageChange() {
    const sortedData = [...globalData].sort((a, b) => b.price_change_percentage_24h - a.price_change_percentage_24h);
    renderTable(sortedData);
  }
  
  // Event listeners
  document.getElementById('searchButton').addEventListener('click', searchData);
  document.getElementById('sortMarketCapButton').addEventListener('click', sortDataByMarketCap);
  document.getElementById('sortPercentageButton').addEventListener('click', sortDataByPercentageChange);
  
  // Initial data fetch
  fetchDataThen(); // You can also use fetchDataAsyncAwait();
  