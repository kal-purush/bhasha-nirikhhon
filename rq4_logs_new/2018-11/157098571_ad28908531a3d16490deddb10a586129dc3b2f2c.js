let currency = "";
while (currency !== "uah" && currency !== "usd")
{
    alert ("Введите валюту, которую вы хотите обменять (uah or usd)");
    currency = prompt("uah or usd").toLowerCase();
    console.log(currency)
}

if (currency === "uah") {
    const X = prompt("Введите число, которое нужно конвертировать: ");
    let uah = parseFloat(X);
    usd = uah/26;
    console.log("USD = " + usd);
    alert("USD = " + usd);
} else {
    const Y = prompt("Write number to convert: ");
    let usd = parseFloat(Y);
    uah = usd*26;
    console.log("UAH = " + uah);
    alert("UAH = " + uah);
}






/* const CURRENCY = "";
while (CURRENCY !== "uah" && CURRENCY !== "usd")
{
    alert ("Введите валюту, которую вы хотите обменять (uah or usd)");
    const CURRENCY = prompt("uah or usd").toLowerCase();
    console.log(CURRENCY)
}

if (CURRENCY === "uah") {
    const X = prompt("Введите число, которое нужно конвертировать: ");
    const UAH = parseFloat(X);
    USD = UAH * 26;
    console.log("USD = " + USD);
    alert("USD = " + USD);
} else {
    const Y = prompt("Write number to convert: ");
    const USD = parseFloat(Y);
    UAH = USD / 26;
    console.log("UAH = " + UAH);
    alert("UAH = " + UAH);
} */