import _ from 'lodash';
import React from 'react';

export default (props) => {
  return (
    <div className="actor">
      <div className="actor__name">{props.encounter.name}</div>
    </div>
  );
};