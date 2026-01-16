import React, { createContext } from 'react';
import propTypes from 'prop-types';

export const UserContext = createContext();

export const UserProvider = ({ user, children }) => (
  <UserContext.Provider value={user}>{children}</UserContext.Provider>
);

UserProvider.propTypes = {
  user: propTypes.object,
  children: propTypes.element,
};