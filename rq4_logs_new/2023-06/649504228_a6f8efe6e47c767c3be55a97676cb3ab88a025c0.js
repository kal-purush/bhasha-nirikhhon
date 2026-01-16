// Filter.js
import React from 'react';

const Filter = ({ onCategoryChange }) => {
  const handleCategoryChange = (event) => {
    const selectedCategory = event.target.value;
    onCategoryChange(selectedCategory);
  };

  return (
    <select onChange={handleCategoryChange}>
      <option value="All">All</option>
      <option value="Fruits">Fruits</option>
      <option value="Vegetables">Vegetables</option>
      <option value="Dairy">Dairy</option>
    </select>
  );
};

export default Filter;