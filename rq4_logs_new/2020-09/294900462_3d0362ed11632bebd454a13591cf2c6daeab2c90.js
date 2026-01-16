import React from "react";
import { BrowserRouter as Router, Route } from "react-router-dom";
import Login from "./pages/login";
import Chat from "./pages/chat";

const App = () => (
  <Router>
    <Route path="/" exact component={Login} />
    <Route path="/chat" component={Chat} />
  </Router>
);

export default App;