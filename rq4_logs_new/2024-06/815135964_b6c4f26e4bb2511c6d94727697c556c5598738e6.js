import React from 'react';
import { Route, Routes } from 'react-router-dom';
import Signin from './login/Signin/Signin';
import Signup from './login/Signup/Signup';
import Signout from './login/Signout/Signout';
import Home from './Home'; // Import the Home component
import { AuthProvider } from './AuthContext'; // Import AuthProvider
import Aboutus from './aboutUs/Aboutus';
import CryptoWebsocket from './CryptoWebsocket';
import CryptoOneDay from './CryptoOneDay';
import CryptoWeek from './CryptoWeek';
import CryptoMonths from './CryptoMonths';
import CryptoThreeMonths from './CryptoThreeMonths';
import Layout from './Layout';
import Ai from './ai/AI'
import DashboardAlert from './DashboardAlert/DashboardAlert';


const App = () => {
  return (
    <AuthProvider>
      <Layout/>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/signin" element={<Signin />} />
        <Route path="/signup" element={<Signup />} />
        <Route path="/signout" element={<Signout />} />
        <Route path="/about" element={<Aboutus />} />
        <Route path="/cryptoWebsocket" element={<CryptoWebsocket />} />
        <Route path="/cryptoOneday" element={ <CryptoOneDay />} />
        <Route path="/cryptoweek" element={<CryptoWeek />} />
        <Route path="/cryptomonth" element={<CryptoMonths />} />
        <Route path="/cryptothreemonths" element={<CryptoThreeMonths/>} />
        <Route path="/ai" element={<Ai/>} />
        <Route path='/alert' element={<DashboardAlert/>}/>
        </Routes>

    </AuthProvider>
    
  );
};

export default App;