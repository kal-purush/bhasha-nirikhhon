import React from 'react';
import Banner from './Banner.component';
import Portfolio from './Portfolio.component';
import Contact from './Contact.component';

console.log('portfolio in infocontainer', Portfolio);

const Body = () => (
  <div style={{ marginTop: '6.9%' }} className="container">
    <Banner />  
    <Portfolio />
    <Contact />
  </div>
);

export default Body;

const textColor = '#777';
const lighterTextColor = '#bbb';

export { textColor, lighterTextColor };