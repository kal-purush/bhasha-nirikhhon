
{
    const checkboxes = document.getElementsByName("checktodo");
    const done_todo = document.getElementsByClassName("done_todo");
    
    for(let i = 0;i < done_todo.length;i++){
        checkboxes[i].addEventListener("change",()=>{
            done_todo[i].classList.toggle("done");
         })
    }
}