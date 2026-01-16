const URL = "https://gist.githubusercontent.com/camperbot/5a022b72e96c4c9585c32bf6a75f62d9/raw/e3c6895ce42069f0ee7e991229064f167fe8ccdc/quotes.json";

const App = () => {
  const [quote, setQuote] = React.useState(null);
  const fetchRandom = async () => {
    const res = await axios.get(URL);
    const quotes = res.data.quotes;
    setQuote(quotes[Math.floor(Math.random() * quotes.length)]);
  };

  React.useEffect(() => {
    fetchRandom();
  }, []);
  return /*#__PURE__*/(
    React.createElement(React.Fragment, null,
    quote && /*#__PURE__*/React.createElement("div", { id: "quote-box" }, /*#__PURE__*/
    React.createElement("div", { id: "icns-wrapper" }, /*#__PURE__*/
    React.createElement("i", { className: "fa-solid fa-quote-left" }), /*#__PURE__*/
    React.createElement("i", { className: "fa-solid fa-quote-right" })), /*#__PURE__*/

    React.createElement("div", { id: "text" }, quote.quote), /*#__PURE__*/
    React.createElement("div", { id: "author" }, "- ", quote.author), /*#__PURE__*/
    React.createElement("div", { id: "btns-wrapper" }, /*#__PURE__*/
    React.createElement("button", {
      onClick: fetchRandom,
      id: "new-quote" }, "New"), /*#__PURE__*/

    React.createElement("a", {
      href: `https://twitter.com/intent/tweet?text="${quote.quote}" – ${quote.author}&hashtags=quotes,${quote.author.replace(/\s/g, '')}`,
      id: "tweet-quote",
      target: "_top",
      rel: "noopener" }, /*#__PURE__*/

    React.createElement("i", { className: "fa-brands fa-twitter" }), " Tweet")))));





};

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render( /*#__PURE__*/React.createElement(App, null));