'use strict';

import React from 'react';
import App from './components/app';
import PageBuilder from './components/PageBuilder';
import Router from 'react-router';
import routes from './components/routes';

Router.run(routes, Router.HistoryLocation, function(Handler) {
    React.render(<Handler />, document.getElementById('root'));
});