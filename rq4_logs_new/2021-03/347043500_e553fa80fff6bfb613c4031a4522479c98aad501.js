// Obtained from our index.html
const button = document.getElementById('button');
const xhr = new XMLHttpRequest();
const users = document.getElementById('users')
// Here we added a click to the event listener
button.addEventListener("click", loadUsers);

// Load Github users
// This is the ajax call
function loadUsers() {
    // This takes the the GET method and the API's location.
  xhr.open("GET", "https://api.github.com/users", true);

  xhr.onload = function () {
    if (this.status === 200) {
        // Here we are parsing JSON data to plain text
      let githubUsers = JSON.parse(this.responseText);
        // Here i used template strings for simplicity
        // output is empty so that you can simply add it later on.
      let output = '';
    //   loop through githubUsers variable because there are many of them
      for(let i in githubUsers){
          output +=
          `<div class = "user">
          <img src= ${githubUsers[i].avatar_url} width=70px height= 70px>
          <ul>
          <li>ID: ${githubUsers[i].id}</li>
          <li>Name: ${githubUsers[i].login}</li>
          </ul>
          </div>`
      }
      users.innerHTML = output;
    }
  };
//   This will give you an output if there is an error
  xhr.onerror = function () {
   alert("Sorry but there is an error");
  };
//   This function helps to send the request.
  xhr.send();
}