(function(){

  let myName = document.getElementById("my-name");
  let getName = document.getElementById("get-name");
  let userName = document.getElementById("user-name");
  let clearStorage = document.getElementById("clearStorage");
  let nameStored = localStorage.name;
  let chosenColour = document.getElementById('colorPicker');
  let colourStored = localStorage.colour;

  // Displays the name in the console at this stage:
  console.log(`Name on page load: ${nameStored}`);

  if(nameStored) {
    // If there's a name in localStorage, use it:
    myName.innerHTML = `again ${nameStored}`;
    console.log(`Name stored is: ${nameStored}`);
  }
  else {
    // There's no name in localStorage:
    myName.innerHTML = "stranger";
    console.log(`No name stored`);
  }
  
  if(colourStored) {
		document.body.style.backgroundColor = colourStored;
	}

  function clearStorageFunc(){
    localStorage.clear();
  }

  function changeBGColour(){
    document.body.style.backgroundColor = chosenColour.value;
	colourStored = chosenColour.value;
	localStorage.colour = colourStored;	
  }

  function PerformGreeting() {
    if(userName.value === "") {
      alert("Please enter a name");
      userName.focus();
    }
    // Gets the name the user entered:
    nameStored = userName.value;

    // Shows the name in "my-name":
    myName.innerHTML = nameStored;

    // Puts the name into localStorage:
    localStorage.name = nameStored;

    // Displays the name in the console at the final stage:
    console.log(`New name stored: ${nameStored}`);

    return false;
  }

  if (typeof event === "undefined") {
    getName.onsubmit = PerformGreeting; // for Firefox
  }
  else {
    getName.addEventListener("submit", PerformGreeting);
    event.preventDefault();
  }

  clearStorage.addEventListener("click",clearStorageFunc);

  colorPicker.addEventListener("change",changeBGColour);

}());