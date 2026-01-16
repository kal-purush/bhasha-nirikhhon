import React from 'react';

export const Product = ({ product, id }) => {
  return (
    <div id={id} className="col-lg-4 col-md-6">
      <div className="product__item">
        <div
          className="product__item__pic product-image"
          style={{ backgroundImage: `url(${product.image})` }}
        >
          <div className="label new">New</div>
          <ul className="product__hover">
            <li>
              <a href={product.image} className="image-popup">
                <span className="arrow_expand"></span>
              </a>
            </li>
            <li>
              <a href="https://wa.me/5215568839532?text=Hola%20estoy%20interesado%20en%20Flagacan%2025kg">
                <span className="icon-whatsapp"></span>
              </a>
            </li>
            <li>
              <a href="#">
                <span className="icon-instagram"></span>
              </a>
            </li>
          </ul>
        </div>
        <div className="product__item__text">
          <h6>
            <a href="#">{product.title}</a>
          </h6>

          <div className="product__price">{product.price}</div>
        </div>
      </div>
    </div>
  );
};