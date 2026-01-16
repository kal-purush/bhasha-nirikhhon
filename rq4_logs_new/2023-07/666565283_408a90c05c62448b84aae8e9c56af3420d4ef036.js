import React, { useState } from "react";

function SearchBar({ search }) {
  const [query, setQuery] = useState("");

  function handleChange(e) {
    setQuery(e.target.value);
    search(e.target.value);
  }

  console.log(query);

  return (
    <form onSubmit={(e) => e.preventDefault()}>
      <input
        type="text"
        onChange={handleChange}
        value={query}
        placeholder="Search for quote or author..."
      ></input>
    </form>
  );
}

export default SearchBar;