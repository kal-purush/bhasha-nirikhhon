import React, { Suspense, lazy } from "react";
import "bootstrap/dist/css/bootstrap.min.css";
import "./App.css";
import { BrowserRouter as Router, Switch, Route } from "react-router-dom";

const Header = lazy(() => import("./routes/Header"));
const Home = lazy(() => import("./routes/Home"));
const ProjectDetails = lazy(() => import("./routes/ProjectDetails"));
const NotFound = lazy(() => import("./routes/NotFound"));

const App = () => {
  return (
    <Suspense fallback={<h3>Loading...</h3>}>
      <Router>
        <div className="App">
          <Header />
          <Switch>
            <Route exact path="/" component={Home} />
            <Route exact path="/project/:id" component={ProjectDetails} />
            <Route exact path="*" component={NotFound} />
          </Switch>
        </div>
      </Router>
    </Suspense>
  );
};

export default App;