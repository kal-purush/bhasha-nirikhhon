import React, { createContext } from 'react';
import useFirebase from './../componenets/Hooks/UseFirebase';

export const authcontext = createContext();

const Authprovider = ({ children }) => {
    const Allcontext = useFirebase()
    return (
        <authcontext.Provider value={Allcontext}>
            {children}
        </authcontext.Provider>
    );
};

export default Authprovider;