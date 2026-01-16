import React from 'react';
import PropTypes from 'prop-types';

const baseUrl = require.context('../assets/img', true);

const Image = ({
  src, srcset, alt,
}) => (
  <picture>
    <source type="image/webp" srcSet={baseUrl(`./${srcset}`)} />
    <img src={baseUrl(`./${src}`)} alt={alt} />
  </picture>
);

Image.propTypes = {
  src: PropTypes.string.isRequired,
  srcset: PropTypes.string.isRequired,
  alt: PropTypes.string.isRequired,
};


export default Image;