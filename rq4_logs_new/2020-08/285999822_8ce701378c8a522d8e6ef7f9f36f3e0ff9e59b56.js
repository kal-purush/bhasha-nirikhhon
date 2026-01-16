import React from "react";
import BookLists from "./components/BookLists";
import AddBook from "./components/AddBook";

function App() {
  return (
    <div id="main">
      <h1>GraphQL Books Library</h1>
      <BookLists />
      <AddBook />
    </div>
  );
}

export default App;