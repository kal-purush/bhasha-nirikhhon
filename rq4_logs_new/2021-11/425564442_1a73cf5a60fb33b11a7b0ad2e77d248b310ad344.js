const input = document.querySelector('.task');
const btnCreate = document.querySelector('.create');

btnCreate.addEventListener('click', createTask)

function createTask() {
    const inputValue = input.value;

    const newDiv = document.createElement("div");
    newDiv.textContent = inputValue;
    
    document.body.appendChild(newDiv);
}