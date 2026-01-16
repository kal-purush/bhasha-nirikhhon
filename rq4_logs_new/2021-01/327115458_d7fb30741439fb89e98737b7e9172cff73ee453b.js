const taskList = document.getElementById('list');

function addTask(){
    const newTask = document.getElementById('task').value;
    const newLi = document.createElement('li');
    const newP = document.createElement('p');
    newP.innerText = newTask;
    newLi.appendChild(newP)
    const removeButton = document.createElement('button');
    removeButton.innerText = "❌";
    newLi.appendChild(removeButton);
    newP.onclick = completedTask;
    removeButton.onclick = removesButton;
    taskList.appendChild(newLi);  
}

function completedTask(event){
    const taskToCross = event.target;
    const task = taskToCross.innerText
    if(taskToCross.classList.contains('strikeThrough')){
        taskToCross.classList.remove('strikeThrough')
        taskToCross.innerText = task.slice(2) ;
    } else {
        taskToCross.classList.add('strikeThrough')
        taskToCross.innerText =  `✓ ${task}`;
    }
}

function removesButton (event) {
    const taskToDel = event.target.parentElement;
    taskList.removeChild(taskToDel);
}