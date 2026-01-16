
import './App.css';
import Heart from './component/heartComponent/heart';
import { useEffect } from 'react';


function App() {

  // useEffect(() => {

  // },[])

  var hearts = [];
 
  // document.body.insertBefore(brd, document.getElementById("check"));
  const duration = 3000;
  const speed = 0.5;
  // const cursorXOffset = 0;
  // const cursorYOffset = -5;


  function generateHeart(x, y, xBound, xStart, scale)
  {
    var brd = document.getElementById('check');
     var heart = document.createElement("DIV");
     heart.setAttribute('class', 'heart');
     brd.appendChild(heart);
     heart.time = duration;
     heart.x = x;
     heart.y = y;
     heart.bound = 30;
     heart.direction = 1;
     heart.style.left = heart.x + "px";
     heart.style.top = heart.y + "px";
     heart.scale = scale;
     heart.style.transform = "scale(" + scale + "," + scale + ")";
     if(hearts == null)
      hearts = [];
     hearts.push(heart);
     
  }

  const handleClick = () => {
    generateHeart(500, 500, null, null, 1);
    var before = Date.now();
    var id = setInterval(frame, 5);
    setTimeout(() =>{
    clearInterval(id);
    },5000)
    frame(before)
  }

  function frame(before )
{
  var current = Date.now();
  var deltaTime = current - before;
  console.log("running again");
   before = current;
   for(var i in hearts)
   {
    var heart = hearts[i];
    heart.time -= 3;
    // console.log("test", heart.time);
    if(heart.time > 0)
    {
      console.log("running in",heart.y,heart.time);
     heart.y -= speed;
     heart.style.top = heart.y + "px";
     heart.style.left = heart.x + heart.direction * heart.bound * Math.sin(heart.y * heart.scale / 30) + "px";
    }
    else
    {
      console.log("running out");
     heart.parentNode.removeChild(heart);
     hearts.splice(i, 1);
    }
   }
}
  
  return (
    <div className='App'>
      <div id="check" className = 'box'>
      {/* <Heart/> */}
      <button onClick={handleClick} style ={{ position: 'relative', top:"25rem", left: "9rem"}}>Heart</button>
      </div> 
    </div>
  );
}

export default App;