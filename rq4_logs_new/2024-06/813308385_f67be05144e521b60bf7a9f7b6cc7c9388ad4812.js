 //BUDGET Functions
 document.addEventListener('DOMContentLoaded', function() {
    var setBudgetButton = document.querySelector('.set-budget-button');
    var overlay = document.querySelector('#budget-amount-overlay');
  
    setBudgetButton.addEventListener('click', function() {
      overlay.classList.toggle('active');
    });
  });

  // Function to assign amount to budget input when preset button is clicked
  function assignAmount(button) {
    var amountInput = document.getElementById("budget-amount");
    var amount = button.getAttribute("data-amount");
    amountInput.value = amount;
}

// Function to confirm budget and update session cookie
function confirmBudget() {
    var amountInput = document.getElementById("budget-amount").value;
    // Save the amount in session cookie
    document.cookie = "budgetAmount=" + amountInput + ";path=/";
    // Close the overlay or make it inactive
    document.getElementById("budget-amount-overlay").style.display = "none";
}

// Function to get budget amount from session cookie
function getBudgetAmountFromCookie() {
    var name = "budgetAmount=";
    var decodedCookie = decodeURIComponent(document.cookie);
    var cookieArray = decodedCookie.split(';');
    for(var i = 0; i < cookieArray.length; i++) {
        var cookie = cookieArray[i];
        while (cookie.charAt(0) === ' ') {
            cookie = cookie.substring(1);
        }
        if (cookie.indexOf(name) === 0) {
            return parseFloat(cookie.substring(name.length));
        }
    }
    return 0;
}

var confirmButton = document.querySelector('.confirm-button');
confirmButton.addEventListener('click', confirmBudget);
  

  function toggleDropdown() {
    const dropdownMenu = document.querySelector('.dropdown-menu');
    dropdownMenu.style.display = dropdownMenu.style.display === 'block' ? 'none' : 'block';
    const chevron = document.querySelector('.chevron');
    chevron.style.transform = dropdownMenu.style.display === 'block' ? 'rotate(180deg)' : 'rotate(0deg)';
}

function selectItem(item) {
    const dropdownButton = document.querySelector('.dropdown-button');
    dropdownButton.innerHTML = item + '<span class="chevron">▼</span>';
    toggleDropdown();
}

document.addEventListener('click', function(event) {
    const dropdownContainer = document.querySelector('.dropdown-container');
    if (!dropdownContainer.contains(event.target)) {
        document.querySelector('.dropdown-menu').style.display = 'none';
        document.querySelector('.chevron').style.transform = 'rotate(0deg)';
    }
});