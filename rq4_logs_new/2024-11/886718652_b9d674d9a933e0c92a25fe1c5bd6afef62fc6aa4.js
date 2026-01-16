import React, { createContext } from 'react';
import UserAPI from '../services/UserApi';

const apiUrl = process.env.REACT_APP_API_URL;
const api = new UserAPI(apiUrl);

export const ApiContext = createContext(api);

export const ApiProvider = ({ children }) => (
    <ApiContext.Provider value={api}>
        {children}
    </ApiContext.Provider>
);