import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import Navbar from './layouts/Navbar';
import Employees from './employees/Employees';
import DialogContextProvider from '../contexts/DialogContext';

function App() {
  return (
    <DialogContextProvider>
      <BrowserRouter>
        <Navbar />
        <Employees />
      </BrowserRouter>
    </DialogContextProvider>
  );
}

export default App;