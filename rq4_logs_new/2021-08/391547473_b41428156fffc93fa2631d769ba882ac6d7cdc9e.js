import "./App.css";
// import HomePage from "./containers/HomeTemplate/HomePage";
// import AboutPage from "./containers/HomeTemplate/AboutPage";
// import ListMoviePage from "./containers/HomeTemplate/ListMoviePage";
import PageNotFound from "./containers/PageNotFound";
import { routesHome } from "./routes";

import { BrowserRouter, Route, Switch } from "react-router-dom";
import Navbar from "./containers/HomeTemplate/_components/Navbar";

function App() {
  const renderLayoutHome = (routtes) => {
    return routtes?.map((item, index) => {
      return (
        <Route
          key={index}
          exact={item.exact}
          path={item.path}
          component={item.component}
        />
      );
    });
  };
  return (
    <BrowserRouter>
      <Navbar />
      <Switch>
        {renderLayoutHome(routesHome)}

        {/* Trang không tồn tại */}
        <Route path="" component={PageNotFound} />
      </Switch>
    </BrowserRouter>
  );
}
export default App;