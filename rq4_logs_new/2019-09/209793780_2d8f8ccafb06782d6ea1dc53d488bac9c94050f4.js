// add new todo

const addNew = document.querySelector('.addTodo');
const addToList = document.querySelector('.todos'); 

// template off the new todo
const generateTemplate = newTodo => {
    const html = `
    <li class="list-group-item d-flex justify-content-between align-items-center">
                <span>${newTodo}</span>
                <i class="fa fa-trash delete"></i>
    </li>
    `;
    addToList.innerHTML+=html;

}

addNew.addEventListener('submit', e =>{

    // stops the default action of an element from happening
    e.preventDefault(); 

    // get the input value
    const newTodo = addNew.addNewTodo.value.trim();
    
    // create a template to add new todo to the list
    generateTemplate(newTodo);
    // reset the input form
    addNew.reset();
});


// delete the todo

addToList.addEventListener('click', e=>{
    // check whether it is click only on the trash icon
    if(e.target.classList.contains('delete')){
        e.target.parentElement.remove();
    }
});
