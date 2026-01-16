import Sidebar from "./components/sidebar/Sidebar";
import Topbar from "./components/topbar/Topbar";
import "./App.css";
import Home from "./pages/home/Home";
import { BrowserRouter as Router, Routes, Route,Navigate } from "react-router-dom";
import UserList from "./pages/userList/UserList";
import User from "./pages/user/User";
import NewUser from "./pages/newUser/NewUser";
import ProductList from "./pages/productList/ProductList";
import Product from "./pages/product/Product";
import NewProduct from "./pages/newProduct/NewProduct";
import Login from "./pages/login/Login";
import { useSelector } from "react-redux";

function App() {
  const admin = useSelector((state) => state.user.currentUser);


  return (
    <Router>
      <Topbar />
      <div className="container">
        {admin != null && admin.isAdmin && <Sidebar />}
        <Routes>
          <Route path="/" element={admin != null && admin.isAdmin ? <Home />: <Navigate to="/login" />}>
          </Route>
          <Route path="/login" element={admin != null && admin.isAdmin  ? <Navigate to="/" /> : <Login/>}>
          </Route>
          <Route path="/users" element={<UserList />}>
          </Route>
          <Route path="/user/:userId" element={<User />}>
          </Route>
          <Route path="/newUser" element={<NewUser />}>
          </Route>
          <Route path="/products" element={<ProductList />}>
          </Route>
          <Route path="/product/:productId" element={<Product />}>
          </Route>
          <Route path="/newproduct" element={<NewProduct />}>
          </Route>
        </Routes>
      </div>
    </Router>

  );
}

export default App;