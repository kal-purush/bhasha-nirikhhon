/******************************************
Treehouse FSJS Techdegree:
project 1 - A Random Quote Generator
******************************************/

// For assistance: 
  // Check the "Project Resources" section of the project instructions
  // Reach out in your Slack community - https://treehouse-fsjs-102.slack.com/app_redirect?channel=chit-chat
 
  // displayedQuotes contains indices of recently displayed quotes
  // It helps keep track of last x quotes to ensure they are not repeated.
  let displayedQuotes = [];
  const QUOTES_PER_QUEUE = 5; // keep track of last x quotes

  const quotes = [
  {
      quote: "Wisely, and slow. They stumble that run fast.",
      source: "William Shakespeare",
      citation: "Romeo and Juliet, Act 2, Scene 3",
      year: 1595
  },
  {
      quote: "Life was meant to be lived, and curiosity must be kept alive. One must never, for whatever reason, turn his back on life.",
      source: "Eleanor Roosevelt",
      citation: "Preface to The Autobiography of Eleanor Roosevelt",
      year: 1960
  },
  {
      quote: "I think that somehow, we learn who we really are and then live with that decision.",
      source: "Eleanor Roosevelt",
      citation: "Peter's Quotations: Ideas for Our Time",
      year: 1972
  },
  {
      quote: "All types of knowledge, ultimately mean self knowledge.",
      source: "Bruce lee",
      citation: "Bruce Lee: The Lost Interview",
      year: 1971
  },
  {
      quote: "E = mc%b2", // hex %b2 is the superscript 2
      source: "Albert Einstein",
      citation: "Annalen der Physik",
      year: 1905
  },
  {
    quote: "Character cannot be developed in ease and quiet. Only through experience of trial and suffering can the soul be strengthened, ambition inspired, and success achieved.",
    source: "Helen Keller",
    citation: "Helen Keller's Journal",
    year: 1938
},
{
    quote: "Make at least one definite move daily toward your goal.",
    source: "Bruce Lee",
    citation: "Jeet Kune Do"
  }
];

// get RandomQuote() - pick, at random, a quote from the quotes array above
function getRandomQuote() {
  var randomQuoteNumber;

  // keep generating random quotes until one is found that
  // has not been displayed in the last few tries.
  while (true) {
    // keep the quotes as random as possible by ensuring the
    // same quote doesn't appear twice in a row
    if (displayedQuotes.length > QUOTES_PER_QUEUE) {
      // remove earliest displayed quote from rotation
      // discard return value
      displayedQuotes.shift();
    }

    randomQuoteNumber = Math.floor(Math.random() * quotes.length);
    if (displayedQuotes.indexOf(randomQuoteNumber) === -1) {
      displayedQuotes.push(randomQuoteNumber); // I just want the index, not the object
      break;
    }
  }
  return quotes[randomQuoteNumber];
}

// getRandomByte() - generate 16 digit value between 0 and 200.
// It will be used to generate colors none of which will fade
// the white text out.
function getRandomByte() {
  return Math.floor(Math.random() * 200);
}

// printQuote() - print a random quote to the web page
function printQuote() {
  let randomQuote = getRandomQuote();
  // unescape any special characters in the quote.
  document.querySelector("p.quote").textContent = unescape(randomQuote.quote);
  // next comes the addition of the source to the page
  document.querySelector(".source").innerHTML = randomQuote.source + 
    "<span class='citation'></span>" + 
    "<span class='year'></span>";
  // add the citation;
  document.querySelector(".citation").textContent = randomQuote.citation;
  // add the year of publication if there is one
  let yearField = document.querySelector(".year");
  if (randomQuote.year !== undefined) {
    yearField.textContent = randomQuote.year;
  } else {
    yearField.textContent = "";
  }
  // change the color of the body's background with each new quote
  let red = getRandomByte();
  let green = getRandomByte();
  let blue = getRandomByte();

  document.body.setAttribute(["style"], "background-color: rgb(" +
    red + "," + green + "," + blue + ");");

}

// add a click event to print a new quote to the page
document.getElementById('load-quote').addEventListener("click", printQuote, false);