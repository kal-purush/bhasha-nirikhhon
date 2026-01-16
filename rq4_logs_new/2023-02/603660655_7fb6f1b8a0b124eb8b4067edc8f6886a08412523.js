import Index from './index.module.css'
import Explore from './pages/Explore/Explore'
import CreateContent from './pages/CreateContent/CreateContent';
import LiveStream from './pages/LiveStream/LiveStream';
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Mainbar from './pages/CreateContent/Inner components/Mainbar';

const App = () => {
  return (
    <BrowserRouter>
      <Routes>
        <Route path='/' element={<Explore/>}/>
          <Route path = "/create" element={<CreateContent/>} />
          <Route path = "/live" element={<LiveStream/>} />
          <Route path = "/create/course-landing" element={<Mainbar/>} />

       
        
      </Routes>
    </BrowserRouter>
  )
}

export default App