import React, { useContext, useState } from "react";
import { Input, Button } from "reactstrap";
import { RibbonContext } from "../../providers/RibbonProvider";

export const RibbonSearch = ({ onSearch }) => {
  const [search, setSearch] = useState("");
  const { searchRibbons, getUserRibbons } = useContext(RibbonContext);

  const handleSearch = (e) => {
    e.preventDefault();
    if (search.trim() !== "") {
      searchRibbons(search)
        .then((res) => res.json())
        .then((searchResults) => onSearch(searchResults));
    } else {
      getUserRibbons()
        .then((res) => res.json())
        .then((searchResults) => onSearch(searchResults));
    }
  };

  return (
    <>
      <div className="my-3 row justify-content-center">
        <Input
          type="text"
          className="w-75"
          placeholder="Search by title, description or snag "
          onChange={(event) => setSearch(event.target.value)}
        />
        <Button onClick={handleSearch}>Search</Button>
      </div>
    </>
  );
};