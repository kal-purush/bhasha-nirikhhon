// import React, { Component } from "react";

// export default class App extends Component {
//   // In this way inside class we can initilize the vaiable and use it with help of this keyword
//   c = "Chetan";
//   render() {
//     return <div>Hello This is class based component {this.c}</div>;
//   }
// }

import React, { useState } from "react";
import NavBar from "./components/NavBar";
import News from "./components/News";
import About from "./components/About";
import Home from "./components/Home";
import { Routes, Route } from "react-router-dom";
import LoadingBar from "react-top-loading-bar";

const App = () => {
  const setPageSize = 30;
  const [progress, setProgress] = useState(0);

  return (
    <>
      <NavBar />
      <LoadingBar color="#f11946" progress={progress} height={3} />
      <div className="container">
        <Routes>
          <Route key={"home"} excat path="/home" element={<Home />}></Route>
          <Route key={"about"} excat path="/about" element={<About />}></Route>
          <Route
            excat
            path="/business"
            element={
              <News
                setProgress={setProgress}
                key={"business"}
                pageSize={setPageSize}
                country="in"
                category="business"
              />
            }
          ></Route>
          <Route
            excat
            path="/entertainment"
            element={
              <News
                setProgress={setProgress}
                key={"entertainment"}
                pageSize={setPageSize}
                country="in"
                category="entertainment"
              />
            }
          ></Route>
          <Route
            excat
            path="/general"
            element={
              <News
                setProgress={setProgress}
                key={"general"}
                pageSize={setPageSize}
                country="in"
                category="general"
              />
            }
          ></Route>
          <Route
            excat
            path="/health"
            element={
              <News
                setProgress={setProgress}
                key={"health"}
                pageSize={setPageSize}
                country="in"
                category="health"
              />
            }
          ></Route>
          <Route
            excat
            path="/science"
            element={
              <News
                setProgress={setProgress}
                key={"science"}
                pageSize={setPageSize}
                country="in"
                category="science"
              />
            }
          ></Route>
          <Route
            excat
            path="/sports"
            element={
              <News
                setProgress={setProgress}
                key={"sports"}
                pageSize={setPageSize}
                country="in"
                category="sports"
              />
            }
          ></Route>
          <Route
            excat
            path="/technology"
            element={
              <News
                setProgress={setProgress}
                key={"technology"}
                pageSize={setPageSize}
                country="in"
                category="technology"
              />
            }
          ></Route>
        </Routes>
      </div>
    </>
  );
};

export default App;