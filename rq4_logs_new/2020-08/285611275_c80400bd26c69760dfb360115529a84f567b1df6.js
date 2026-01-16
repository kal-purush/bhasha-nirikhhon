let addToy = false;

// document.addEventListener("DOMContentLoaded", () => {
  const addBtn = document.querySelector("#new-toy-btn");
  const toyFormContainer = document.querySelector(".container");
  addBtn.addEventListener("click", () => {
    // hide & seek with the form
    addToy = !addToy;
    if (addToy) {
      toyFormContainer.style.display = "block";
    } else {
      toyFormContainer.style.display = "none";
    }
  });
// });
let toyForm = document.querySelector(".add-toy-form")
//gets the toy collection div
let toyDiv = document.querySelector("#toy-collection")
//set the class name
toyDiv.className = "card"
//creates a blank h2 tag
let blankH2 = document.createElement("h2")
//add the blank h2 tag to the div
toyDiv.append(blankH2)
//creates a blank img tag
let image = document.createElement("img")
//add the blank img tag to the div
toyDiv.append(image)
//creates a blank p tag
let pTag = document.createElement("p")
//add the blank p tag to the div
toyDiv.append(pTag)
//creates a blank button
let button = document.createElement("button")
//set the class name
button.className = "like-btn"
//set the inner text
button.innerText = "Like <3"
//sets class name
image.className = "toy-avatar"

toyForm.addEventListener("submit", (form) => {
  // prevents refreshing of page prior to completion of element loading
  form.preventDefault

  let toyName = form.target.name.value
  let toyImage = form.target.image.value

  fetch('http://localhost:3000/toys', {
    method: 'POST', // or 'PUT'
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      name: toyName,
      image: toyImage
    }),
  })
  .then(response => response.json())
  .then((newToy) => {
    console.log(newToy);
  })
  
})



});