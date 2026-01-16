// JavaScript Document

// Creating the Function for Calculation of the Result
function calculateTip() {
    // Used to obitain the input data from the User and using it for calculation
    const amountBillElement = document.getElementById('amountBill');
    const percentageTipElement = document.getElementById('percentageTip');
    const peopleNumberElement = document.getElementById('peopleNumber');

    //Parsing the data values
    const billAmount = Math.abs(parseInt(amountBillElement.value));
    const tipPercentage = Math.abs(parseInt(percentageTipElement.value));
    const peolpeNumber = Math.abs(parseInt(peopleNumberElement.value));

    // Storing the obitained data values in the assigned Variables
    amountBillElement.value = billAmount;
    percentageTipElement.value = tipPercentage;
    peopleNumberElement.value = peolpeNumber;

    // Applying the calculations and formulas
    const totalTip = Math.floor((billAmount * tipPercentage)) / 100;
    const totalToPay = (billAmount + totalTip)/peolpeNumber;
    const tipPerPerson = Math.floor(totalTip * 100 / peolpeNumber) / 100;
 
    // Generation of Final Output
    document.getElementById('totalPay').innerText = totalToPay;
    document.getElementById('perPerson').innerText = tipPerPerson;
}