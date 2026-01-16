/*
 Firebase Authentication Working Example, JavaScript, CSS, and HTML crafted with love and lots of coffee.
 (c) 2016, Ron Royston, MIT License
 https://rack.pub
 
 1. ..................................................................INITIALIZE
    FIREBASE CONFIG
    INITIALIZE FIREBASE WEB APP
    REDEFINE GLOBAL DOCUMENT AS LOCAL DOC
 2. ...................................................................VARIABLES
    ACCOUNT PAGE
    ACK / EMAIL ACTION HANDLER PAGE
    PRIVATE PAGE
    PUBLIC PAGE
    SHARED
 3. .............................................................EVENT LISTENERS
    ACCOUNT PAGE
    PRIVATE PAGE
    SHARED
    SOCIAL MEDIA BUTTONS
 4. ............................................................FIREBASE METHODS
    INITIALIZE FIREBASE WEB APP
    FIREBASE AUTH STATE CHANGE METHOD
 5. ...................................................................FUNCTIONS
    ACCOUNT PAGE
    ACK / EMAIL ACTION HANDLER PAGE
    LOGIN PAGE
    PUBLIC PAGE 
    SHARED
 6. ............................................................REVEALED METHODS
    ADD NODES WITH DATA TO REALTIME DATABASE
*/


/*

INITIALIZE

*/
document.addEventListener('DOMContentLoaded', function() {
  
  //REDEFINE DOCUMENT AS LOCAL DOC
  var doc = document;
  window.snackbarContainer = doc.querySelector('#toast');
  
  /*
  
  VARIABLES
  
  */

  //ACCOUNT PAGE
  var introSection = doc.getElementById('intro');
  var deviceSection = doc.getElementById('device');
  var addDeviceButton = doc.getElementById('add-device');
  
  //SHARED
  
  var drawer = doc.getElementsByClassName('mdl-layout__drawer')[0];
  var navLinks = drawer.getElementsByClassName('mdl-navigation')[0];
  
  deviceSection.style.display = "none";
  
  
  addDeviceButton.addEventListener("click", function(){
    introSection.style.display = "none";
    deviceSection.style.display = "inline";
  });
  

  //LOCAL STORAGE TEST
  Object.defineProperty(this, "ls", {
    get: function () { 
      var test = 'test';
      try {
        localStorage.setItem(test, test);
        localStorage.removeItem(test);
        return true;
      } catch(e) {
        return false;
      }
    }
  });

  /*
  
  EVENT LISTENERS
  
  */
  
  //ACCOUNT PAGE

  //enable pressing enter
  // if(newEmailInputMdlTextfield){
  //   newEmailInputMdlTextfield.addEventListener("keyup", function(e) {
  //     e.preventDefault();
  //     if (e.keyCode == 13) {
  //       newEmailSubmitButton.click();
  //     }
  //   });
  // }


  function getArg(param) {
    var vars = {};
    window.location.href.replace( location.hash, '' ).replace( 
      /[?&]+([^=&]+)=?([^&]*)?/gi, // regexp
      function( m, key, value ) { // callback
        vars[key] = value !== undefined ? value : '';
      }
    );
    if ( param ) {
      return vars[param] ? vars[param] : null;	
    }
    return vars;
  }


  function addPrivateLinkToDrawer(){
    if(!doc.getElementById('private-link')){
      var icon = doc.createElement("i");
      var iconText = doc.createTextNode('lock_outline');
      var anchorText = doc.createTextNode(' Private');
      icon.classList.add('material-icons');
      icon.appendChild(iconText);
      
      var privateLink = doc.createElement("a");
      privateLink.classList.add('mdl-navigation__link');
      privateLink.href = "../intrest/private";
      privateLink.id = "private-link";
      privateLink.appendChild(icon);
      privateLink.appendChild(anchorText);
      
      var anchorList = navLinks.getElementsByClassName('mdl-navigation__link')[1];
      navLinks.insertBefore(privateLink, anchorList);
    }
  }

  function removePrivateLinkFromDrawer(){
    var linkToRemove = doc.getElementById('private-link');
    if(linkToRemove){
      linkToRemove.parentNode.removeChild(linkToRemove);
    }
  }
  
  function redirect(path){
    //console.log('redirect ran with path = ' + path);
    var baseURL = window.location.protocol + '//' + window.location.host + '/firebase-auth';
    var hasSlash = path.charAt(0) == '/';
    
    //if(path == '/') {
    //  path = baseURL;
    //}
    
    if(!hasSlash){
      path = '/' + path;
    }
    //console.log ('hasSlash = ' + hasSlash + ', and path = ' + path);
    //console.log(baseURL + path); // https://rack.pub/firebase-authhttps://rack.pub/firebase-auth
    var onThisPage = (window.location.href.indexOf(baseURL + path) > -1);
    
    if (!onThisPage) {
      //redirect them to login page for message
       location = baseURL + path;
    }
  }
  
  
  // Build out page
  
    //read localStorage
  
      //for each thing
        //print it with loader gif and promise then fetch the data
  

  
  
  
  //else display Add New Card
    // options full or third width card
    // type of display - guage, bar, table
    // data to fetch  
  
  
  
  
  

  //TOAST
  function toast (msg,timeout){
    if(!timeout){timeout = 2750}
    var data = {
      message: msg,
      timeout: timeout
    };
    snackbarContainer.MaterialSnackbar.showSnackbar(data);
  };

// END
}, false);

/*

REVEALED METHODS

*/

//REVEALED METHOD TO ADD NODES WITH DATA TO REALTIME DATABASE
//eg, demo.update('mynode','myKey','myValue')
var demo = (function() {
  var pub = {};
  pub.update = function (node,key,value){
    var ref = firebase.database().ref('/');
    var obj = {};
    obj[key] = value;
    ref.child(node).update(obj)
    .then(function() {
      console.log('Update Ran Successfully');
    });       
  }
  //API
  return pub;
}());