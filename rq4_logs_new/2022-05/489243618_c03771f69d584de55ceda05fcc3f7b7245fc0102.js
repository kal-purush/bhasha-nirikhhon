import { useState, useEffect, useRef } from "react";
import Pricechart from "./Pricechart";
import './Dashboard.css';

function Dashboard() {
    const [selectOptions, setselectOptions] = useState([]);
    const [selectedPair, setselectedPair] = useState("BTC-USD");
    const [realtimePrice, setrealtimePrice] = useState("0.00");
    const ws = useRef(null);
    
    // Fill select menu with options from API 
    useEffect(() => {
        const url = "https://api.pro.coinbase.com";
        let pairs = [];
    
        const fetchAPIdata = async () => {
            await fetch(url + "/currencies").then((res) => res.json()).then((data) => (pairs = data));
    
            // Filter data by USD
            pairs = pairs.filter((pair) => {
                if (pair.id !== "USD" && pair.id !== "USDC") {
                    return pair;
                }
                return null;
            });
    
            // Filter base_currency in alphabetical order
            pairs = pairs.sort((a, b) => {
                if (a.id < b.id) {
                    return -1;
                }
                if (a.id > b.id) {
                    return 1;
                }
                return 0;
            });
    
            // Change currencies useState
            setselectOptions(pairs);
        };
        fetchAPIdata();
    }, []);
      
    const handleSelect = (event) => {
        let unsubMsg = {
            type: "unsubscribe",
            product_ids: [selectedPair],
            channels: ["ticker"],
        };
    
        // When selectedPair changes, unsubscribe from previous pair
        unsubMsg = JSON.stringify(unsubMsg);
        ws.current.send(unsubMsg);
        setselectedPair(event.target.value);
    };

    useEffect(() => {
        ws.current = new WebSocket("wss://ws-feed.pro.coinbase.com");
        let msg = {
            type: "subscribe",
            product_ids: [selectedPair],
            channels: ["ticker"],
        };
        msg = JSON.stringify(msg);
        // Once connection open, send message
        ws.current.onopen = function() {
            ws.current.send(msg);
        };
        ws.current.onmessage = (event) => {
            let data = JSON.parse(event.data);
            if (data.type !== "ticker") {
              return;
            }
            // Update real time price
            if (data.product_id === selectedPair) {
              setrealtimePrice(parseFloat(data.price).toFixed(2));
            }
        };
    },[selectedPair]);
    
    return (
        <div id="mainDash" className="row">
            <div id="realtime" className="col-md-3">
                <div id="price" className="row">
                    <p className="col-7 col-sm-12">Real-time Price in USD</p>
                    <h1 className="col-5 col-sm-12">{`$${realtimePrice}`}</h1>
                </div>
                <div id="menu">
                    <select id="selection" name="currency" value={selectedPair} onChange={handleSelect}>
                        {selectOptions.map((cur, idx) => {
                            return (
                                <option key={idx} value={cur.id + "-USD"}>
                                    {cur.id + " (" + cur.name + ")"}
                                </option>
                            );
                        })}
                    </select>
                </div>
            </div>
            <div id="chart" className="col-md-9">
                <Pricechart pair={selectedPair}/>
            </div>
        </div>
    );
}

export default Dashboard;