function bindKeys(){
let left = keyboard("ArrowLeft"),
    up = keyboard("ArrowUp"),
    right = keyboard("ArrowRight"),
    down = keyboard("ArrowDown"); 

    left.press = function(){bunny.vx = -5;};
    right.press = function(){bunny.vx = 5;};
    up.press = function(){bunny.vy = -5;};
    down.press = function() {bunny.vy = 5;};

    left.release = function(){
        console.log("early Release!");
        if(!right.isDown){
            bunny.vx = 0;
        }
    };
    right.release = function(){
        if(!left.isDown){
            bunny.vx = 0;
        }
    };
    up.release = function(){
        if(!down.isDown){
            bunny.vy = 0;
        }
    };
    down.release = function(){
        if(!up.isDown){
            bunny.vy = 0;
        }
    };
}


//Key bind function
function keyboard(value) {
  let key = {};
  key.value = value;
  key.isDown = false;
  key.isUp = true;
  key.press = undefined;
  key.release = undefined;
  //The `downHandler`
  key.downHandler = event => {
    if (event.key === key.value) {
      if (key.isUp && key.press) key.press();
      key.isDown = true;
      key.isUp = false;
      event.preventDefault();
    }
  };

  //The `upHandler`
  key.upHandler = event => {
    if (event.key === key.value) {
      if (key.isDown && key.release) key.release();
      key.isDown = false;
      key.isUp = true;
      event.preventDefault();
    }
  };

  //Attach event listeners
  const downListener = key.downHandler.bind(key);
  const upListener = key.upHandler.bind(key);
  
  window.addEventListener(
    "keydown", downListener, false
  );
  window.addEventListener(
    "keyup", upListener, false
  );
  
  // Detach event listeners
  key.unsubscribe = () => {
    window.removeEventListener("keydown", downListener);
    window.removeEventListener("keyup", upListener);
  };
  
  return key;
}