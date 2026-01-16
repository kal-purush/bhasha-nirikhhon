const toggleList = document.getElementById('toggleList');
const listDiv = document.querySelector('.list');
const descriptionInput = document.querySelector('input.description');
const descriptionP = document.querySelector('p.description');
const descriptionButton = document.querySelector('button.description');
const listUl = listDiv.querySelector('ul');
const addItemInput = document.querySelector('input.addItemInput');
const addItemButton = document.querySelector('button.addItemButton');
const removeItemButton = document.querySelector('button.removeItemButton');
const todoNode = document.querySelector("#todo");
const buttonNode = document.querySelector("#button");
const lis = listUl.children;
const firstListItem = listUl.firstElementChild;
const lastListItem = listUl.lastElementChild;

function attachListItemButtons(li){
    let up = document.createElement('button');
    up.className = 'up';
    up.textContent = 'Up';
    li.appendChild(up);
    
    let down = document.createElement('button');
    down.className = 'down';
    down.textContent = 'Down';
    li.appendChild(down);
  
    let remove = document.createElement('button');
    remove.className = "remove";
    remove.textContent = 'Remove';
    li.appendChild(remove);
  
  
  }

  for(let i = 0; i < lis.length;i++){
    attachListItemButtons(lis[i]);
  }

  listUl.addEventListener('click', (event) => {
    if (event.target.tagName == 'BUTTON') {
      if(event.target.className == 'remove'){
        let li = event.target.parentNode; 
        let ul = li.parentNode; 
        ul.removeChild(li);
      }
      if(event.target.className == 'up'){
        let li = event.target.parentNode;
        let prevLi = li.previousElementSibling;
        let ul = li.parentNode;
        if(prevLi){
         ul.insertBefore(li, prevLi);
       }
      }
      if(event.target.className == 'down'){
        let li = event.target.parentNode;
        let nextLi = li.nextElementSibling;
        let ul = li.parentNode;
        if(nextLi){
         ul.insertBefore(nextLi, li);
       }
      }
    }
  });
  
  
  toggleList.addEventListener('click', () => {
    if (listDiv.style.display == 'none') {
      toggleList.textContent = 'Hide list';
      listDiv.style.display = 'block';
    } else {
      toggleList.textContent = 'Show list';                        
      listDiv.style.display = 'none';
    }                         
  });
  
  descriptionButton.addEventListener('click', () => {
    descriptionP.innerHTML = descriptionInput.value + ':';
    descriptionInput.value = '';
  });
  
  addItemButton.addEventListener('click', () => {
    let ul = document.getElementsByTagName('ul')[0];
    let li = document.createElement('li');
    li.textContent = addItemInput.value;
    attachListItemButtons(li);
    ul.appendChild(li);
    addItemInput.value = '';
  });
    
  removeItemButton.addEventListener('click', () => {
    let ul = document.getElementsByTagName('ul')[0];
    let li = document.querySelector('li:last-child');
    ul.removeChild(li);
  });

var taskCompleted = 0

function showList() {
    const xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status == 200) {
            let response = JSON.parse(this.response);
            let data = "";
            for (var i = 0; i < response.length; i++) {
                if (response[i].completed == true) {
                    data += `<li class="list-group-item list-group-item-primary">
                       <input id ="checkBox${i + 1}" class="form-check-input me-1" type="checkbox" value="" checked disabled>
                       <i class='bx bx-checkbox-minus'></i>
                       <label>${response[i].title}</label>
                     </li>`;
                } else {
                    data += `<li class="list-group-item list-group-item-secondary">
                    <input id ="checkBox${i + 1}" class="form-check-input me-1" type="checkbox" value="" onchange="checkTodo(${i + 1})">
                    <i class='bx bx-checkbox-minus'></i>
                    <label>${response[i].title}</label>
                  </li>`;
                }
            }
            todoNode.innerHTML = data;
            buttonNode.innerHTML = "";
        }
    }
    xhttp.open("GET", "https://jsonplaceholder.typicode.com/todos", true);
    xhttp.send();
}



function checkTodo(num) {
    const checkBoxNode = document.querySelector(`#checkBox${num}`);
    if (checkBoxNode.checked) {
        taskCompleted += 1;
    } else {
        taskCompleted -= 1;
    }
    if (taskCompleted == 5) {
        alert("Congrats. 5 Tasks have been Successfully Completed.");
    }
}