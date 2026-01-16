window.onload = go;
function go() {
  document.getElementById("new-quote").click(function () {
    newQuote();
  });
}

function newQuote() {
  var quotes = [
    {
      id: 1,
      text: "When you are 4-0 up you should never lose 7-1.",
      source: "Lawrie McMenemy",
    },

    {
      id: 2,
      text: "We must have had 99 percent of the game. It was the other three percent that cost us the match.",
      source: "Ruud Gullit",
    },

    {
      id: 3,
      text: "I don’t believe in superstitions. I just do certain things because I’m scared in case something will happen if I don’t do them.",
      source: "Michael Owen",
    },

    {
      id: 4,
      text: "Well, Clive, it’s all about the two M’s—movement and positioning.",
      source: "Ron Atkinson",
    },

    {
      id: 5,
      text: "They’re the second-best team in the world, and there’s no higher praise than that.",
      source: "Kevin Keegan",
    },

    {
      id: 6,
      text: "If you don’t believe you’re the best, then you will never achieve all that you are capable of.",
      source: "Cristiano Ronaldo",
    },
  ];

  var randomQuote = quotes[Math.floor(Math.random() * quotes.length)];
  document.getElementById("text").innerHTML = randomQuote.text;
  document.getElementById("author").innerHTML = randomQuote.source;
  document
    .getElementById("#tweet-quote")
    .attr(
      "href",
      stringToClickToTweetURL(
        '"' + randomQuote.text + '" - ' + randomQuote.source
      )
    );
}

function stringToClickToTweetURL(str) {
  // Convert to Click to Tweet URL
  var stringToConvert = str
    .split(" ")
    .join("%20")
    .split("@")
    .join("%40")
    .split("!")
    .join("%21");

  // Put 'Click to Tweet' URL suffix at the begining
  var resultString = "https://twitter.com/intent/tweet?text=" + stringToConvert;

  // Return properly formatted "Click to Tweet URL"
  return resultString;
}