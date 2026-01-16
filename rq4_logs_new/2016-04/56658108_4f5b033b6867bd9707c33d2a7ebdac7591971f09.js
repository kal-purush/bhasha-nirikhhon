import React, { Component, PropTypes } from 'react';
import ReactDom, { render } from 'react-dom';

export default class PanelGroup extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
  	const {
      children
  	} = this.props;

    let isActive = (active ? 'active' : '')

    return (
      <div class="panel-group">
        {children}
      </div>    
    );
  }
}