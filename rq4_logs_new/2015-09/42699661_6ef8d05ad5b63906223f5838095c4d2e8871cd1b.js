import React from 'react';
import styles from './sub-heading.less';

export default React.createClass({
  displayName: 'Sub Heading',

  render() {
    return (
      <h1 className={styles[this.props.subHeadingType]}>{this.props.text}</h1>
    );
  }
});