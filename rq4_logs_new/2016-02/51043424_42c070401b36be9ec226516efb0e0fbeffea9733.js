import React, { Component, PropTypes } from 'react';
import { Row, Col } from '../../../dist';

export default class Grid extends Component {

  static propTypes = {
    value: PropTypes.string
  };

  render () {
    const { value } = this.props;

    return (
      <Row>
        <Col size={3}>
          <button>{value}</button>
        </Col>
        <Col size={3}>
          <button>{value}</button>
        </Col>
        <Col size={6}>
          <Row>
            <Col size={6}>YAR!</Col>
            <Col size={6}>{value}</Col>
          </Row>
        </Col>
      </Row>
    );
  }
}