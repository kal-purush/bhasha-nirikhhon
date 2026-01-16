import React from 'react';
import { Row, Col } from 'react-flexbox-grid';
import './styles/FeaturedProducts.css';

const placeholderProducts = [
  {
    barcode: '0000000000001',
    product_name: 'Kahvi',
    price: 50
  },
  {
    barcode: '0000000000002',
    product_name: 'Kokkelikola',
    price: 180
  },
  {
    barcode: '0000000000003',
    product_name: 'Yhden tähden Jallu',
    price: 700
  },
  {
    barcode: '0000000000004',
    product_name: 'Suolapähkinät',
    price: 150
  }
];

class FeaturedProductInfo extends React.Component {
  render() {
    const product = this.props.product;

    return <li>
      <Row>
        <Col xs={8}>{product.product_name}</Col>
        <Col xs={4} className="product-price">
          {(product.price / 100).toFixed(2)} &euro;
        </Col>
      </Row>
    </li>;
  }
}

export default class FeaturedProducts extends React.Component {
  render() {
    const productList = placeholderProducts.map((product) =>
      <FeaturedProductInfo product={product}/>
    );

    return <div className="featured-products">
      <h2>Suositut tuotteet</h2>
      <ul>{productList}</ul>
    </div>;
  }
}