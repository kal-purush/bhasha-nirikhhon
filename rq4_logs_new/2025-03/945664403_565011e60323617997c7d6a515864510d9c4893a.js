(async function() {
    // Wait for the page to load
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Step 1: Extract the Question
    let questionElement = document.querySelector(".question-text"); // Update selector if needed
    if (!questionElement) {
        console.error("Question element not found.");
        return;
    }
    
    let questionText = questionElement.innerText.trim();
    console.log("Extracted Question:", questionText);

    // Step 2: Fetch Answer from Wolfram Alpha API
    let wolframAppId = "QT2P6U-64HYVYUAK3"; // Replace with your Wolfram Alpha API Key
    let apiUrl = `https://api.wolframalpha.com/v2/query?input=${encodeURIComponent(questionText)}&format=plaintext&output=JSON&appid=${wolframAppId}`;

    try {
        let response = await fetch(apiUrl);
        let data = await response.json();
        
        // Extract answer from API response
        let answer = data.queryresult.pods?.[1]?.subpods?.[0]?.plaintext;
        if (!answer) {
            console.error("Failed to extract answer from API.");
            return;
        }

        console.log("Solution:", answer);

        // Step 3: Open a New Tab with the Answer
        let newTab = window.open("about:blank", "_blank");
        if (newTab) {
            newTab.document.write(`<html><body><h1>Solution</h1><p>${answer}</p></body></html>`);
        } else {
            console.error("Popup blocked. Allow popups and try again.");
        }

    } catch (error) {
        console.error("Error fetching or processing answer:", error);
    }
})();