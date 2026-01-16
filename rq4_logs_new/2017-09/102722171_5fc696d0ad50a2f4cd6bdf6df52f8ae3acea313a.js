import React from 'react';
import { renderRoutes } from 'react-router-config';
import PropTypes from 'prop-types';
import Header from './component/Header';
import './assets/bootstrap.min.css';
import './assets/jquery-3.2.1.slim.min.js';
import './assets/popper.min.js';
import './assets/bootstrap.min.js';

const Main = ({ route }) => (
  <div>
    <Header />
    {renderRoutes(route.routes)}
  </div>
);

Main.propTypes = {
  route: PropTypes.object.isRequired
};

export default Main;