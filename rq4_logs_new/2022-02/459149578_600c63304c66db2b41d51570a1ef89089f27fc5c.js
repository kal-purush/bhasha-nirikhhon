import "./App.css";
import { Switch, Route } from "react-router-dom";
import ListRecord from "./components";
import AddEdit from "./components/AddEdit";
import Error from "./components/Error";
import Header from "./components/Header";
import View from "./components/View";
import About from "./components/About";

function App() {
  return (
    <div className="App">
      <Header />
        <Switch>
          <Route exact path="/" component={ListRecord} />
          <Route exact path="/add" component={AddEdit} />
          <Route exact path="/view/:id" component={View} />
          <Route exact path="/update/:id" component={AddEdit} />
          <Route exact path="/about" component={About} />
          <Route path="*" component={Error} />
        </Switch>
      
      {/* </div> */}
    </div>
  );
}

export default App;