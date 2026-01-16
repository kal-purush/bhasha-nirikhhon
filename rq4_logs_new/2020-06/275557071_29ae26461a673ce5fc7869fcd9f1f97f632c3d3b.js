import React, { useState } from 'react';
import './App.css';
import { BrowserRouter } from 'react-router-dom';
import Header from './components/Header/Header'
import Main from './components/Main/Main'
import LanguageContext from './languageContext';
import 'semantic-ui-css/semantic.min.css'
import Footer from './components/Footer/Footer'





const App = () => {
    
    const [lang, setLang] = useState('pl');

    return (
    <BrowserRouter>
        <LanguageContext.Provider value={lang}>
            <Header onLanguageChange={setLang} rel="stylesheet" href="//cdn.jsdelivr.net/npm/semantic-ui@2.4.2/dist/semantic.min.css" />
            <Main />
            <Footer />
        </LanguageContext.Provider>
    </BrowserRouter>
);
    };


export default App;