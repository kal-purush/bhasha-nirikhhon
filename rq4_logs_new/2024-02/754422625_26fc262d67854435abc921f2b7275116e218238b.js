const button = document.querySelector("#js-button");

button.addEventListener("click", getQuote);

const endpoint = "https://api.quotable.io/random";

async function getQuote() {
	try {
		const response = await fetch(endpoint);
		const json = await response.json();

		if (!response.ok) {
			throw Error(response.statusText);
		}

    // const jsonContent = json.content
    console.log(json.content);
    displayQuote(json.content)
	} catch (error) {
		console.log(error);
		alert("failed to fetch quote");
	}

  function displayQuote(quote) {
    const quoteUl = document.querySelector('#js-quote-ul') 

    const quoteItem = document.createElement('li')
    quoteItem.textContent = quote

    quoteUl.append(quoteItem)
  }
}