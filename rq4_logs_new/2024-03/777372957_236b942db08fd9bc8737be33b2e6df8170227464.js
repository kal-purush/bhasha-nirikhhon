const API_KEY = "ZDgZcihAnXDiHgjRFTzyqkE1qMmptukT";
const API_URL = (ticker) => `https://api.polygon.io/v2/aggs/ticker/${ticker}/prev?apiKey=${API_KEY}&interval=1m`;

async function fetchLatestStockPrice(ticker) {
    // Initiate the API request
    const response = await fetch(API_URL(ticker));
    const data = await response.json();

    // Check for the errors in the response
    if (data.error || !data || !data.results || !data.results[0])
        return alert("There was an error!");

    const out = data.results[0];
    return data.results[0];
}

// Register click handler for button
document.getElementById("check").addEventListener("click", async () => {
    // Get ticker and check it
    const ticker = document.getElementById("ticker").value;
    if (!ticker)   
        return alert("You cannot leave the ticker blank.");

    // Request the API for the stock
    const stock = await fetchLatestStockPrice(ticker);

    // Did it work?
    if (stock) {
        // Set student ID
        document.getElementById("name").innerText = 'Name: Matthew Contaldi | ID: 1227490';

        // Set docs url
        document.getElementById("docs").innerText = "Docs: https://polygon.io/docs/stocks/getting-started";

        // Set data
        document.getElementById("open").innerText = `Open: $${stock.o}`;
        document.getElementById("high").innerText = `High: $${stock.h}`;
        document.getElementById("low").innerText = `Low: $${stock.l}`;
        document.getElementById("close").innerText = `Close: $${stock.c}`;
        document.getElementById("volume").innerText = `Volume: $${stock.v}`;

        // Speak it (the project says to display the data in another format that isn't just text. This is not text, it's audio)
        const utterance = new SpeechSynthesisUtterance(`The stock's low is ${stock.l} dollars, and high is ${stock.h} dollars. The stock's open is ${stock.o} dollars, and close is ${stock.c} dollars. Lastly, the stock's volume is ${stock.v} dollars.`);
        window.speechSynthesis.speak(utterance);
    }
});