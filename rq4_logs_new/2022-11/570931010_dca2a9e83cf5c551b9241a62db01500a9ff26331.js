import Navbar from './components/Navbar';
import AddPost from './components/AddPost';
import Footer from './components/Footer';
import Home from './components/Home';
import EditPost from './components/EditPost';
import './App.css';
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import ViewPost from './components/ViewPost';
import Error from './components/Error';


function App() {
  return (
    <>
      <Router>
        <Navbar />
        
        <Routes>
          <Route path="/"
            element={<Home />} />

          <Route path="/addPost"
            element={<AddPost />} />

          <Route path="/editPost/:id"
            element={<EditPost />} />

          <Route path="/viewPost/:id"
            element={<ViewPost />} />

          <Route path="*" element={<Error />} />
        </Routes>

        <Footer />
      </Router>
    </>
  );
}

export default App;