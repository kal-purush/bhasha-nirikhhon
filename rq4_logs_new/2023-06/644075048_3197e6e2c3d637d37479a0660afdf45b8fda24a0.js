
//tudo que e relacionado a atualizacao da tela seria view

export default class TaskView{
    constructor(removeTaskCb){
        this.removeTaskCb = removeTaskCb
    }
    tasksTemplate(tasks){
    }

    taskTemplate(task){
        return `
            <li class="taskItem" data-name="${task}">
                <button class="taskItemBtn"></button>
                <span class="taskItemText">${task}</span>
            </li>
        `
    }

    render(tasks) {
        tasks.forEach(task => {
            this.renderOne(task)
        })
    }   

    renderOne(task) {
        const taskList = document.getElementById('tasksList') //ul
        taskList.innerHTML += this.taskTemplate(task)
        setTimeout(() => {this.addListenerToRemoveItem(task), 50})
    }

    addListenerToRemoveItem(task) {
        const item = document.querySelector(`[data-name="${task}"]`)
        const button = item.querySelector('.taskItemBtn')
        
        button.addEventListener('click', () => {   
            console.log('clicou')         
            this.removeTaskCb(task)
            this.removeTask(task)
        })
    }

    //task é uma string - nome da task
    removeTask(task){
        //todos os li
        const item = document.querySelector(`[data-name="${task}"]`)
        console.log(item)
            const taskText = item.querySelector('.taskItemText').textContent
            if(taskText === task){
                console.log('achei')
                item.classList.add('fadeOut')
                setTimeout(() => item.remove(), 1200)
            }
        }
}