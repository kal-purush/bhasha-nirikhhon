import React from 'react';
import { Admin } from 'admin-on-rest';
import restClient from './restClient';

import resources from './resources';
import { API_HOST, API_PATH } from './config/_entrypoint';

const API_URL = (API_HOST + API_PATH).replace(/\/$/, '');

const App = () => (
  <Admin
    restClient={restClient(API_URL)}>
    {resources}
  </Admin>
);

export default App;