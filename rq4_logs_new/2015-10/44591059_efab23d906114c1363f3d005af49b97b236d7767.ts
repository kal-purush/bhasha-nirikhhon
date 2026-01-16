/// <reference path="../../typings/react/react.d.ts"/>

import * as React from 'react';
import { TestComponent } from './TestComponent';

document.onload = () => {
  console.log('run');
  React.render(React.createElement(TestComponent), document.getElementById('main'));
};