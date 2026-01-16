/******************************************
Treehouse FSJS Techdegree:
project 1 - A Random Quote Generator
******************************************/

// For assistance: 
  // Check the "Project Resources" section of the project instructions
  // Reach out in your Slack community - https://treehouse-fsjs-102.slack.com/app_redirect?channel=chit-chat

/*** 
 * `quotes` array 
***/
const quotes = [
  {
    quote: 'A life spent making mistakes is not only more honorable, but more useful than a life spent doing nothing.',
    source: 'George Bernhard Shaw',
    citation: 'https://medium.com/swlh/21-of-the-worlds-most-powerful-quotes-updated-for-today-and-tomorrow-6b7634358c2',
  },
  {
    quote: 'Light travels faster than sound. This is why some people appear bright until you hear them speak.',
    source: 'Alan Dundes',
    citation: 'https://www.keepinspiring.me/funny-quotes/',
    year: 'N/A'
  },
  {
    quote: "It's funny how I see it more straight when I'm bent", 
    source: 'Joey Bada$$',
    citation: 'Album: 1999',
    year: '2012'
  },
  {
    quote: "Two things are infinite: the universe and human stupidity; and I'm not sure about the universe.",
    source: 'Albert Einstein',
    citation: 'https://www.goodreads.com/quotes',
    year: 'N/A'
  },
  {
    quote: 'A room without books is like a body without a soul',
    source: 'Marcus Tullius Cicero',
    citation: 'https://www.goodreads.com/quotes',
    year: 'N/A'
  }
]



/***
 * `getRandomQuote` function
***/
function getRandomQuote (arr) {
  let randomNum = Math.floor(Math.random() * arr.length);
  let quote = arr[randomNum];
  return quote

}
/***
 * `printQuote` function
***/
function printQuote(){
  let quote = getRandomQuote(quotes);
  let msg = '';
  msg += `<p class='quote'> ${quote.quote} </p>`;
  msg += `<p class='source'> ${quote.source} `;
  if (quote.source){
    msg += `<span class="citation"> ${quote.citation} </span>`; /* if the quote has a source */
  };
  if(quote.year){
    msg += `<span class="year"> ${quote.year} </span> `;/*if the quote has a year*/
  };
  msg += `</p>`

  let randomValue = () => Math.floor(Math.random() * 256); /* this will get the rgb value for the background*/
  function randomRGB(value){
    let backGround = `rgb( ${value()} , ${value()} , ${value()} )` /*all the color code to a string*/
    return backGround;
  }
  document.body.style.backgroundColor = randomRGB(randomValue) /*this will call the function to get the background color */
  return document.getElementById('quote-box').innerHTML = msg;
  
}
setInterval(printQuote,15000); /*timer*/


/***
 * click event listener for the print quote button
 * DO NOT CHANGE THE CODE BELOW!!
***/

document.getElementById('load-quote').addEventListener("click", printQuote, false);