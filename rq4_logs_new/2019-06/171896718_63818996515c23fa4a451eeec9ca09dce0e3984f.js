import React from 'react';
import ReactDOM from 'react-dom';
import classNames from 'classnames';

import './style.postcss';

class Dialog extends React.Component {
  render() {
    const {children, onClickMask} = this.props;
    return ReactDOM.createPortal(
      <div className={classNames('dialog')}>
        <div className={classNames('dialog-body')}>
          <div onClick={onClickMask} className={classNames('dialog-mask')} />
          <div className={classNames('dialog-body-container')}>{children}</div>
        </div>
      </div>,
      document.getElementById('root')
    );
  }
}

export default Dialog;