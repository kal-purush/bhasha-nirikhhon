import React, { Component, Suspense } from "react";
import Header from "./Components/Header";
import scrollToComponent from "react-scroll-to-component";
import styled, { createGlobalStyle } from "styled-components";

import Millimynd from "./Components/Millimynd";
import { Break } from "./Components/Utilities/break";
import SideDrawer from "./Components/SideDrawer/SideDrawer";
import UmGunnar from "./Components/UmGunnar";

const Background = React.lazy(() =>
  import("./Components/Millistig/Leftsidecontainer")
);
const Showcase = React.lazy(() => import("./Components/Showcase"));
const Upplysingar = React.lazy(() => import("./Components/Upplysingar"));

const GlobalStyles = createGlobalStyle`
html {
  overflow-x: hidden;
}
  body {
    margin: 0;
    font-family: 'Roboto', serif !important;
  }
`;

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      sideDrawerOpen: false
    };
  }

  drawerToggleClickHandler = e => {
    this.setState(prevState => {
      return { sideDrawerOpen: !prevState.sideDrawerOpen };
    });
  };

  _scrollToDiv = e => {
    switch (e.currentTarget.id) {
      case "vorur":
        scrollToComponent(this.Products, { duration: 1500 });
        break;
      case "umOkkur":
        scrollToComponent(this.Gunnso, { duration: 1500 });
        break;
      case "synishorn":
        scrollToComponent(this.Showcase, { duration: 1000 });
        break;
      default:
        return this.Products;
    }
  };

  render() {
    return (
      <AppDiv>
        <GlobalStyles />
        <Header
          button={this._scrollToDiv}
          drawerClickHandler={this.drawerToggleClickHandler}
        />
        <Break />
        <Suspense fallback={<div>Loading...</div>}>
          <Background
            ref={section => {
              this.Products = section;
            }}
          />
        </Suspense>

        <Break />
        <Suspense fallback={<div>Loading...</div>}>
          <Showcase
            ref={section => {
              this.Showcase = section;
            }}
          />
        </Suspense>

        <Break />
        <Millimynd />
        <Break />
        <UmGunnar />
        <Break />

        <Suspense fallback={<div>Loading...</div>}>
          <Upplysingar
            ref={section => {
              this.Gunnso = section;
            }}
          />
        </Suspense>

        <SideDrawer
          drawerClickHandler={this.drawerToggleClickHandler}
          show={this.state.sideDrawerOpen}
          button={this._scrollToDiv}
        />
      </AppDiv>
    );
  }
}

export default App;

const AppDiv = styled.div`
  margin: 0;
  padding: 0;
  text-align: center;
  height: 100%;
  width: 100vw;
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
`;