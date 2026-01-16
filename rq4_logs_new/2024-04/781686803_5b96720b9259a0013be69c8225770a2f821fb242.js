const btnAdicionarTarefa = document.querySelector('.app__button--add-task');
const formAdicionarTarefa = document.querySelector('.app__form-add-task');
const textareaTarefa = document.querySelector('.app__form-textarea');

const tarefas = [];

btnAdicionarTarefa.addEventListener('click', () => {
    formAdicionarTarefa.classList.toggle('hidden');
})

formAdicionarTarefa.addEventListener('submit', (e) => {
    e.preventDefault();

    tarefa = {
        descricao: textareaTarefa.value
    }

    const listaTarefas = localStorage.getItem('tarefas');

    if (listaTarefas && listaTarefas.includes(JSON.stringify(tarefa))) {
        alert('Tarefa repetida')
        return;
    }

    tarefas.push(tarefa);
    localStorage.setItem('tarefas', JSON.stringify(tarefas));
})