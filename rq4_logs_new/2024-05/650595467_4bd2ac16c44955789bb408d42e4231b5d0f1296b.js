// Categories.js
import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { Link } from 'react-router-dom';

const Categories = () => {
  const [categories, setCategories] = useState([]);

  useEffect(() => {
    const fetchCategories = async () => {
      const response = await axios.get('https://fakestoreapi.com/products/categories');
      setCategories(response.data);
    };

    fetchCategories();
  }, []);

  return (
    <div>
      {categories.map((category, index) => (
        <div key={index}>
          <Link to={`/products/${category}`}>{category}</Link>
        </div>
      ))}
    </div>
  );
};

export default Categories;