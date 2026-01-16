// When ever input field is changed this function will execute
function handleChange(id) {
    const first = document.getElementById(id);
    if (first.value < 0 || isNaN(parseInt(first.value))) {
        alert("You can assign only Positive Integer number here.");
        first.value = 0;
    }
    calculateAndRender();
}

// Handle the event when increment or decrement button is clicked
function handleClick(couchClass, type) {
    const inputField = document.getElementById(couchClass);
    const parsedValue = parseInt(inputField.value);
    if (type == "increase") {
        inputField.value = parsedValue + 1;
        handleChange("first-class");
    } else if (type == "decrease" && parsedValue > 0) {
        inputField.value = parsedValue - 1;
        handleChange("first-class");
    }
}

// Calculate total price with vat and render in HTML
//pass 'popup' for calculate and render with popup
function calculateAndRender(popup) {
    // DOM variables
    const firstClassInput = document.getElementById("first-class");
    const economyClassInput = document.getElementById("economy-class");

    // Couch Ticket Price
    const firstClassTicketPrice = 150;
    const economyClassTicketPrice = 100;

    // Parsed Value
    const firstClassTicketCount = parseInt(firstClassInput.value);
    const economyClassTicketCount = parseInt(economyClassInput.value);

    // Sub total, vat and total Calculation
    const subTotal =
        firstClassTicketPrice * firstClassTicketCount +
        economyClassTicketPrice * economyClassTicketCount;
    const vat = subTotal * 0.1;
    const total = subTotal + vat;

    // check weather its render for popup or main section
    let string = "";
    if (popup == "popup") {
        string = "popup-";
    }

    // Required Fields to Show Cost
    const subTotalField = document.getElementById(string + "sub-total");
    const vatField = document.getElementById(string + "vat");
    const totalField = document.getElementById(string + "total");
    const bookingBtn = document.getElementById("booking-btn");

    // Injecting values in HTML
    subTotalField.innerText = subTotal;
    vatField.innerText = vat;
    totalField.innerText = total;

    // On condition Button will affect and response
    if (total == 0) {
        bookingBtn.href = "#";
        bookingBtn.style.cursor = "no-drop";
        bookingBtn.style.backgroundColor = "#5e5858";
        bookingBtn.style.color = "#c9c2c2";
    } else {
        bookingBtn.href = "#popup";
        bookingBtn.style.cursor = "pointer";
        bookingBtn.style.backgroundColor = " #e33741";
        bookingBtn.style.color = "white";
    }
}

// Initialize app
calculateAndRender();