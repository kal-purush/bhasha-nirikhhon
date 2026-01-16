import logo from './logo.svg';
import './App.css';
import SignUp from './SignUp';

function App() {
  return (
    <div style={{
      backgroundImage: "linear-gradient(45deg, #A75D5D, #0081B4)",
      backgroundSize: "100%",
      backgroundRepeat: "repeat",
  }} className="">
      <SignUp></SignUp>
    </div>
  );
}

export default App;