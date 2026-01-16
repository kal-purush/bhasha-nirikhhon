import React, { Dispatch, useEffect, useState } from "react";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

import BagSushi from "../Asset/BagSushi.PNG";
import menu from "../Asset/iconmonstr-menu-lined-240.png";
import fond1 from "../Asset/Desktop - 1 (2).png";
import Sushi from "../Asset/iMac - 1.png";
import saumon from "../Asset/iMac - 2.png";
import Avatar from "../Asset/94041140-femme-asiatique-avatar-icône-femme-japonaise-dans-le-profil-traditionnel-profil-plat-illustration-ve.webp";
import "./App.css";

function App() {
  const [MobileNavbar, setMobileNavbar] = useState(false);

  const notify = () => {
    toast.info("Ce site possède une version mobile !", {
      position: "top-right",
      autoClose: 8000,
      hideProgressBar: false,
      closeOnClick: true,
      pauseOnHover: true,
      draggable: true,
      progress: undefined,
      toastId: "ok",
    });
  };

  notify();

  return (
    <div className="App">
      {MobileNavbar ? (
        <>
          <div className="App-NavBarMobil">
            <div className="App-NavBarMobilContainer">
              <a
                className="hover-underline-animation"
                onClick={() => setMobileNavbar(!MobileNavbar)}
              >
                Menu
              </a>
              <a className="hover-underline-animation">Food</a>
              <a className="hover-underline-animation">Service</a>
              <a className="hover-underline-animation">About Us</a>
            </div>
          </div>
        </>
      ) : (
        <>
          <ToastContainer />
          <div className="ContainerLeft">
            <div className="ContainerLeft1">
              <div className="App-NavBarDesktop">SUSHIMAN</div>
              <img src={fond1}></img>
              <div className="ContainerLeft2">
                <img src={saumon}></img>
                <img src={Sushi}></img>
              </div>
            </div>
          </div>
          <div className="ContainerRight">
            <div className="App-NavBarDesktop2">
              <a>Menu</a>
              <a>Food</a>
              <a>Service</a>
              <a>About Us</a>
            </div>
            <div className="App-NavBar">
              <div className="App-NavBar1">
                {" "}
                <p>
                  <b>
                    <span>SUSHI</span>MAN
                  </b>
                </p>
              </div>
              <div
                className="App-NavBar2"
                onClick={() => setMobileNavbar(!MobileNavbar)}
              >
                <img src={menu}></img>
              </div>
            </div>
            <div className="App-H1">
              <h1>Feel the taste of Japanese foods</h1>
            </div>

            <div className="App-ContaineurH1Description">
              <img src={BagSushi} id="App-Sushi"></img>
              <p>
                Feel the taste of most populars Japanese foods from anywhere and
                anytime.
              </p>
            </div>

            <div className="App-ContaineurBtn">
              <button id="btn1">Order Now</button>
              <button id="btn2">How to Order</button>
            </div>

            <div className="App-ContaineurInfoBar">
              <div className="App-ContaineurInfoBar2">
                <h2>24K+ </h2> <h3> Happy Customer</h3>
              </div>
              <div className="App-ContaineurInfoBar3">
                <img src={Avatar}></img>{" "}
                <p>
                  <i>
                    " This is the best japanese food delivery service that ever
                    exist "
                  </i>
                </p>
              </div>
            </div>

            <div className="App-AboutUsContainer">
              <h2>About Us / 私たちに関しては</h2>

              <h1>Our mission is to bring true japanese flavours to you.</h1>

              <p>
                {" "}
                We will continue to provide the experience of Hokaido, the
                Japanese mindset of hospitality, with our shopping and dining
                for our customers.
              </p>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default App;