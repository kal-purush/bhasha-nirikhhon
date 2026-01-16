import { createElement } from "./scripts/createElement.js"; // Permite o import de mais de uma funcionalidade dentro das chaves
// import createElement from "./createElement.js"; // import padrão => permite o não uso das chaves por exportar uma única funcionalidade
import { validateInput, tasklistListener } from "./scripts/validation.js";

const addTaskButton = document.querySelector('.tasklist__button');
const taskInput = document.querySelector('#task');
const warningMessage = document.querySelector('.tasklist__warning');
var tasksArray = [];

// Ouvinte para o botão de adicionar task
addTaskButton.addEventListener('click', () => {
    return validateInput(warningMessage, taskInput);
});

// Ouvinte para o teclado
taskInput.addEventListener('keydown', (e) => {

    // Quando pressionar Enter, chama a função validateField()
    if (e.key === 'Enter') {
        validateInput(warningMessage, taskInput);
    };

});

// Adiciona a Task ao HTML
export function addTask() {
    
    createElement(taskInput);
    
    // Se o checkbox estiver marcado, aplica o estilo riscado e itálico
    // P.S.: a completar...
    // const checkbox = document.querySelector('#check__input');
    // checkbox.addEventListener('click', () => {
    //     if(checkbox.checked) {
    //         console.log("check!");
    //     } else {
    //         // console.log("");
    //     }
    // });

    // Adiciona a tarefa ao array tasksArray
    tasksArray.push(taskInput.value);

    // Aciona a função para verificar se o array está vazio
    tasklistListener(tasksArray);
    
};

tasklistListener(tasksArray);