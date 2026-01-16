// create 


// let task=[{}];
let add=document.querySelector('#add')
// let main_div=document.querySelector('main');
let task_list=document.querySelector('main');
let field=document.querySelector('#todo');
let counter=document.getElementById('counter');
let  con=0;

function new_task(){
    if (!field.value){return}
    con ++;//counter increase
    //added item 
    let dbox=document.createElement('div');
    dbox.className='dbox';
    //the text
    let item=document.createElement('p');
    let item_to_add=document.createTextNode(field.value);
    item.appendChild(item_to_add);
    //add delete button to each element 
    let del=document.createElement('button');
    del.className='del';
    del.innerHTML='del';
    //display all elemnts
    dbox.appendChild(item);
    task_list.appendChild(dbox);
    dbox.appendChild(del);

    field.value='';
    counter.innerHTML=con;
    del.addEventListener('click',remove)
}
add.addEventListener('click',new_task);
function remove (){
    // reduce counter by one
    this.parentNode.classList.add('dele');
    let clear=()=>{this.parentNode.remove()};
    const myTimeout=setTimeout(clear,280)
    con--;
    counter.innerHTML=con;
}
