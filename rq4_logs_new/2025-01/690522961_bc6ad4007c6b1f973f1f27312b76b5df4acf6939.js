document.getElementById('calculator-form').addEventListener('submit', function(event) {
  event.preventDefault();

  const initialInvestment = parseFloat(document.getElementById('initial-investment').value);
  const withdrawalRate = parseFloat(document.getElementById('withdrawal-rate').value) / 100;
  const inflationRate = parseFloat(document.getElementById('inflation-rate').value) / 100;
  const returnRate = parseFloat(document.getElementById('return-rate').value) / 100;
  const startDate = new Date();

  // Calculation logic
  let portfolio = initialInvestment;
  let withdrawal = initialInvestment * withdrawalRate;
  let years = 0;
  let withdrawalInYears = [];
  let monthlyWithdrawalInYears = [];
  let withdrawalDates = [];
  let remainingBalances = [];
  let maxYears = 150; // Set limit to 150 years
  let resultLimit = 50; // Show only first 50 years in results dropdown

  while (portfolio > 0) {
    let withdrawalDate = new Date(startDate);
    withdrawalDate.setFullYear(startDate.getFullYear() + years);
    withdrawalDates.push(withdrawalDate.toISOString().split('T')[0]);

    if (years === 0) {
      portfolio -= withdrawal;
    } else {
      portfolio -= withdrawal;
      portfolio *= (1 + returnRate);
      withdrawal *= (1 + inflationRate);
    }

    remainingBalances.push(Math.max(0, portfolio));
    withdrawalInYears.push(withdrawal);
    monthlyWithdrawalInYears.push(withdrawal / 12);
    
    years++;
    if (portfolio <= 0) break;
  }

  // Display the number of years the withdrawals last
  document.getElementById('years-count').innerText = years;

  // Display the starting income (monthly withdrawal for year 1)
  const startingIncome = monthlyWithdrawalInYears[0].toFixed(2);
  document.getElementById('starting-income').innerText = `With your starting income of ₹${startingIncome} per month.`;

  // Displaying results, limit to 50 years if more than 50 years
  const resultContainer = document.getElementById('results-container');
  resultContainer.innerHTML = '';  // Clear previous results

  // Loop through years and display only up to 50 years
  for (let i = 0; i < Math.min(years, resultLimit); i++) {
    const div = document.createElement('div');
    div.className = 'result-item';
    div.innerHTML = `
      <p><span>Year ${i + 1} (${withdrawalDates[i]}):</span></p>
      <p>Annual Withdrawal: ₹${withdrawalInYears[i].toFixed(2)}</p>
      <p>Monthly Withdrawal: ₹${monthlyWithdrawalInYears[i].toFixed(2)}</p>
      <p>Remaining Portfolio: ₹${remainingBalances[i].toFixed(2)}</p>
    `;
    resultContainer.appendChild(div);
  }

  // Show "Limited to 50 years" message if applicable
  if (years > maxYears) {
    const limitMessage = document.createElement('p');
    limitMessage.className = 'red-text';
    limitMessage.textContent = "Calculations are limited to 50 years.";
    resultContainer.appendChild(limitMessage);
  }

  document.querySelector('.result').style.display = 'block';
});

// Toggle the visibility of the results
document.getElementById('toggle-results').addEventListener('click', function() {
  const resultsContainer = document.getElementById('results-container');
  const isVisible = resultsContainer.style.display === 'block';
  
  // Toggle visibility
  resultsContainer.style.display = isVisible ? 'none' : 'block';
  
  // Update button text
  this.textContent = isVisible ? 'Show Year Wise Calculations' : 'Hide Year Wise Calculations';
});