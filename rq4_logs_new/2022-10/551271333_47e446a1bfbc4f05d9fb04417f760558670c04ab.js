import './App.css';
import ProductList from './components/ProductList';
import ProductForm from './components/ProductForm';
import React, { useState } from 'react';

const products = [
  {
    pID: 1, productName: "Product 1", productDescription: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua", productPrice: 1299
  },
  {
    pID: 2, productName: "Product 2", productDescription: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua", productPrice: 1299
  },
  {
    pID: 3, productName: "Product 3", productDescription: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua", productPrice: 1299
  },
  {
    pID: 4, productName: "Product 4", productDescription: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua", productPrice: 1299
  },
  {
    pID: 5, productName: "Product 5", productDescription: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua", productPrice: 1299
  },
]

function App() {

  const [newProductList, updateProductList] = useState(products)

  const onSubmitGetData = (newProductDetails) => {
    newProductDetails.pID = newProductList.length + 1;
    updateProductList([newProductDetails, ...products]);
  }


  return (
    <div className='h-100 d-flex align-items-center justify-content-center flex-column'>
      <ProductForm onSubmit={onSubmitGetData} />
      <ProductList items={newProductList} />
    </div>
  )
}

export default App;