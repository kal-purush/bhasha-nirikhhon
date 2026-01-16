import React from 'react';
import ClassNames from 'classnames';

import '../scss/loading-state.scss';

const LoadingState = ({ paneltype, msg }) => {
  
  const classes = ClassNames({
    'hubdb-infopanel': true,
    'hubdb-infopanel--error': paneltype === 'error',
    'hubdb-infopanel--loading': paneltype === 'loading'
  });

  return(
    <div className={classes}>
      <div className="hubdb-infopanel--liner">
       <p>{msg}</p>
      </div>
    </div>
  );
};

export default LoadingState;