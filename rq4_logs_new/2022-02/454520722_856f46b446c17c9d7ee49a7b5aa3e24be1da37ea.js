import { useState } from 'react/cjs/react.development';
import './App.css';
import Navbar from './components/Navbar';
import TextBox from './components/TextBox';
// import About from './components/About'



function App() {
  const [mode, setMode] = useState("light");

  const toggleMode = () =>{
    if(mode === 'light')
    {
      setMode('dark');
      document.body.style.backgroundColor = '#112644';
    }
    else{
      setMode('light');
      document.body.style.backgroundColor = '#F0F2F5';
      
    }
  }
  return (
    <>
   <Navbar title= "Text App" about="About Text App" mode={mode} toggleMode={toggleMode}/>
   <TextBox mode={mode}/>
   {/* <About /> */}
   </>
  );
}

export default App;