
import './App.css';

import React ,{useState} from "react";
const App =()=> {
  
  const[count,setcount] = useState(0)
 
  const handleIncrement=()=>{
  setcount(count+1)
}
const handleDecrement=()=>{
  setcount(count-1)
}
  return<>
 <h1 className='a'>counter app</h1>
  <div className='b'>counter:{count}</div> <br></br>
  <div className='q'><button  onClick={handleIncrement} className='c'>Increment</button>
  <button onClick={handleDecrement}  className='d'>Decrement</button>
  <button onClick={()=>setcount(0)} className='e'>Reset</button></div>

  </>
  };
export default App;


