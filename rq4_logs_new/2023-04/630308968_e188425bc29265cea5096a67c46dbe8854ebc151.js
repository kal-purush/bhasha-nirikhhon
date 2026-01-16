import "./App.css";
import { Route, Routes } from "react-router-dom";
import Home from "./page/Home";
import BoardList from "./page/BoardList";
import Layout from "./page/Layout";
import Board from "./page/Board";
import BoardWriteForm from "./page/BoardWriteForm";
import { DataProvider } from "./context/DataContext";
import DataContext from "./context/DataContext";
import BoardModifyForm from "./page/BoardModifyForm";
import LoginForm from "./page/LoginForm";
import ImagePage from "./page/ImagePage";
function App() {
  return (
    <div className="App">
      <DataProvider>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route path="/" element={<Home />} />
            <Route path="/boardlist" element={<BoardList />} />
            <Route path="/boardlist/:id" element={<Board />} />
            <Route path="/boardwriteform" element={<BoardWriteForm />} />
            <Route path="/board-modify" element={<BoardModifyForm />} />

            <Route path="/loginform" element={<LoginForm />} />
            <Route path="/image" element={<ImagePage/>}/>
          </Route>
        </Routes>
      </DataProvider>
    </div>
  );
}

export default App;