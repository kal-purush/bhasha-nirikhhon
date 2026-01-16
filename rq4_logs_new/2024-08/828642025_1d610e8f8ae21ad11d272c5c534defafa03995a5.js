import React, { useState } from 'react';
import Popup from './popup';
import './App.css';


const ApiKeys = () => {
    const [alphaKey, setAlphaKey] = useState('');
    const [polygonKey, setPolygonKey] = useState('');
    const [finnhubKey, setFinnhubKey] = useState('');
    const [showPopup, setShowPopup] = useState(false);

    const handleSave = async (e) => {
        e.preventDefault(); 
        try {
            // Save Alpha Vantage API Key
            if (alphaKey) {
                await fetch('https://localhost:8443/api/setApiKey', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ source: 'alpha_vantage', apiKey: alphaKey }),
                    credentials: 'include',
                });
            }

            // Save Polygon API Key
            if (polygonKey) {
                await fetch('https://localhost:8443/api/setApiKey', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ source: 'polygon', apiKey: polygonKey }),
                    credentials: 'include',
                });
            }
            if (finnhubKey) {
                await fetch('https://localhost:8443/api/setApiKey', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ source: 'polygon', apiKey: polygonKey }),
                    credentials: 'include',
                });
            }
            alert('API keys have been saved successfully.');
            setShowPopup(true);
        } catch (error) {
            console.error('Error saving API keys:', error);
            alert('Failed to save API keys.');
        }
    };

    return (
       
        <div>
            <h1>Enter API Keys</h1>
            <form onSubmit={handleSave}>
            <div>
                <label>Alpha Vantage API Key:</label>
                <input
                    type="text"
                    value={alphaKey}
                    onChange={(e) => setAlphaKey(e.target.value)}
                />
            </div>
            <div>
                <label>Polygon API Key:</label>
                <input
                    type="text"
                    value={polygonKey}
                    onChange={(e) => setPolygonKey(e.target.value)}
                />
                
            </div>
            <div>
                <label>Finnhub API Key:</label>
                <input
                    type="text"
                    value={finnhubKey}
                    onChange={(e) => setFinnhubKey(e.target.value)}
                />
                
            </div>
            <button onSubmit={handleSave}>Save API Keys</button>
            </form>
           
            {showPopup && <Popup onClose={() => setShowPopup(false)} />}
            
        </div>
      
    );
};

export default ApiKeys;