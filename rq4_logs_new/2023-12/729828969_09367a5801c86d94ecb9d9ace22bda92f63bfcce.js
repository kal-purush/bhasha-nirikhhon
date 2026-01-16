import "./App.scss";
import Footer from "./components/footer/Footer";

import Navbar from "./components/navbar/Navbar";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import ProductsList from "./components/products/ProductsList";
import Electronics from "./components/pages/Electronics";
import Jewelery from "./components/pages/Jewelery";
import Men from "./components/pages/Men";
import Women from "./components/pages/Women";

function App() {
  return (
    <BrowserRouter>
      <Navbar />
      <Routes>
        <Route path="/" element={<ProductsList />} />
        <Route path="electronics" element={<Electronics />} />
        <Route path="jewelery" element={<Jewelery />} />
        <Route path="men" element={<Men />} />
        <Route path="women" element={<Women />} />
      </Routes>
      <Footer />
    </BrowserRouter>
  );
}

export default App;