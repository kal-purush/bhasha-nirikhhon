import React, { useContext, useState } from "react";

import { ethers } from "ethers";

import { TourismeContext } from "../App";
import { TourTokenContext } from "../App";

import { Web3Context } from "../hooks/useWeb3";

const GetTokens = () => {
  const Tourisme = useContext(TourismeContext);
  const TourToken = useContext(TourTokenContext);

  const [amount, setAmount] = useState("0");
  const [addrAgence, setAddrAgence] = useState("0x0");
  const [getTokens, setGetTokens] = useState(false);

  const nbTokens = ethers.utils.parseEther("500.0");
  const handleGetTokens = async () => {
    await TourToken.mint(addrAgence, nbTokens);
  };

  return (
    <>
      <div className="introText">
        <div className="row">
          <div className="col-lg-6">
            <h2>Welcome</h2>
            <p>You are registered.</p>
          </div>
          <div className="col-lg-6 links">
            <button onClick={() => setGetTokens(!getTokens)}>Get tokens</button>
          </div>
          <div className="getTokens">
            {getTokens && (
              <>
                <hr />
                <form>
                  <label>Agency Address :</label>
                  <input
                    value={addrAgence}
                    onChange={(e) => {
                      setAddrAgence(e.currentTarget.value);
                    }}
                  />
                  {/*  <label>Amount in token :</label>
                  <input
                    value={amount}
                    onChange={(e) => {
                      setAmount(e.currentTarget.value);
                    }}
                  /> */}
                  <button type="button" onClick={handleGetTokens}>
                    Get Tokens
                  </button>
                </form>
              </>
            )}
          </div>
        </div>
      </div>
    </>
  );
};

export default GetTokens;