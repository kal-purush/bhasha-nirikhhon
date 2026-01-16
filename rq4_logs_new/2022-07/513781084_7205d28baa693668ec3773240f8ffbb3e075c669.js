import Login from "./pages/login/Login";
import Home from "./pages/home/Home";
import Profile from "./pages/profile/Profile";
import Register from "./pages/register/Register";
import { BrowserRouter as Router, Navigate, Route, Routes } from 'react-router-dom';
import { useContext } from "react";
import { AuthContext } from "./state/AuthContext";



function App() {
//関数の中で記述
  const { user } = useContext(AuthContext);

  return (
    //ルーティング設定
    <Router>
      <Routes>
        {/* userが存在すればホーム、存在しなければ登録画面 */}
        <Route path="/" element={user ? <Home /> : <Register/>}/>

        {/* userが存在すればホームにリダイレクト */}
        <Route path="/login" element={user ? <Navigate to="/" /> : <Login />}/>

        <Route path="/register" element={<Register />}/>
        <Route path="/profile/:username" element={<Profile />}/>
      </Routes>
    </Router>
  );
}

export default App;
//index.jsでimport