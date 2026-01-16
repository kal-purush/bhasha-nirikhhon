import React from 'react';
import PropTypes from 'prop-types';
import Render from './Render';

class Image extends React.Component {
  static propTypes = {
    styles: PropTypes.shape({}),
  };

  constructor(props, context) {
    super(props, context);
  }

  render() {
    const options = this.props;

    return Render.bind(this)(options);
  }

}

export default Image;