class Mapping {
  constructor(public input, public mapping, public pressed:boolean){

  }
}

class Controller {
  input = {};
  mapping = {};

  constructor(controls){
    for(var key in controls){
      var m = new Mapping(controls[key].toLowerCase(), key.toLowerCase(), false);
      this.input[controls[key]] = m;
      this.mapping[key] = m;
    }

    var setterFunc = (bool)=>{
      return ( e ) => {
          var hit =  convertToKey(e.keyCode)
          for(var key in this.input){
            if(key == hit){
              this.input[key].pressed = bool;
              break;
            }
          }
      }
    }

    document.addEventListener('keyup', setterFunc(false));
    document.addEventListener('keypress', setterFunc(true));
  }

  isDown(control){
    return this.mapping[control].pressed
  }
}

function convertToKey(keycode){
  return String.fromCharCode(keycode).toLowerCase()
}

export default Controller