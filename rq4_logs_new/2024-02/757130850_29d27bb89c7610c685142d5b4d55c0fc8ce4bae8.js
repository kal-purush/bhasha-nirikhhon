import React, { useEffect, useState } from 'react';
import axios from 'axios';
import './Breakdown.css'


const BreakdownPage = () => {
  const [categoryBreakdown, setCategoryBreakdown] = useState([]);

  useEffect(() => {
    axios.get('http://localhost:4000/breakdown') 
      .then(response => {
        setCategoryBreakdown(response.data);
        console.log(response.data
          )
      })
      .catch(error => {
        console.error('Error fetching category breakdown:', error);
      });
  }, []);

  return (
    <div className="page">
      <h2>Breakdown Page</h2>
      <ul className="category-list">
        {categoryBreakdown.map(category => (
          <li key={category._id} className="category">
            {category._id}: ${category.totalAmount}
          </li>
        ))}
      </ul>
    </div>
  );
};

export default BreakdownPage;