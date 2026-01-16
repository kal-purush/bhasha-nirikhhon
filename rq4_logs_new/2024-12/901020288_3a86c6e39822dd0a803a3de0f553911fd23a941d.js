// ATM Logic
let balance = 500; // Initial balance
let userPIN = "1234"; // Default PIN
let transactionMode = null;

// Enter PIN Function
function enterPIN() {
    const enteredPIN = document.getElementById("pin-input").value;
    const screenText = document.getElementById("screen-text");

    if (enteredPIN === userPIN) {
        screenText.innerText = "PIN Accepted! Choose an action.";
        document.getElementById("actions").style.display = "flex";
        document.querySelector('.input-section').style.display = "none";
    } else {
        screenText.innerText = "Invalid PIN! Try again.";
    }
}

// Check Balance Function
function checkBalance() {
    document.getElementById("screen-text").innerText = `Your balance is: $${balance}`;
}

// Deposit Function
function deposit() {
    transactionMode = "deposit";
    document.getElementById("form-section").style.display = "flex";
    document.getElementById("screen-text").innerText = "Enter amount to deposit.";
}

// Withdraw Function
function withdraw() {
    transactionMode = "withdraw";
    document.getElementById("form-section").style.display = "flex";
    document.getElementById("screen-text").innerText = "Enter amount to withdraw.";
}

// Confirm Transaction
function confirmTransaction() {
    const amount = parseFloat(document.getElementById("amount-input").value);
    if (isNaN(amount) || amount <= 0) {
        alert("Please enter a valid amount.");
        return;
    }

    if (transactionMode === "deposit") {
        balance += amount;
        document.getElementById("screen-text").innerText = `Deposited $${amount}. New balance: $${balance}.`;
    } else if (transactionMode === "withdraw") {
        if (amount > balance) {
            alert("Insufficient balance!");
        } else {
            balance -= amount;
            document.getElementById("screen-text").innerText = `Withdrawn $${amount}. New balance: $${balance}.`;
        }
    }
    cancelTransaction();
}

// Cancel Transaction
function cancelTransaction() {
    document.getElementById("form-section").style.display = "none";
    document.getElementById("amount-input").value = "";
}

// Exit Function
function exit() {
    document.getElementById("screen-text").innerText = "Thank you! Please take your card.";
    document.getElementById("actions").style.display = "none";
    document.querySelector('.input-section').style.display = "flex";
    document.getElementById("pin-input").value = "";
}