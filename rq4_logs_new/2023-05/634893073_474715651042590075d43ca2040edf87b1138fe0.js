const add=document.querySelector('.button-add');
const sort=document.querySelector('.novbe');
const list=document.querySelector('.list');
let sorted=false;

function addListItem(info){
    const li= document.createElement('li');
    const input=document.createElement('input');
    input.classList.add('input')
    input.value=info;

    const del=document.createElement('button');
    del.classList.add('li-btn');
    del.addEventListener('click',e=>{
        e.target.parentElement.remove();
    });
    li.append(input,del);
    list.append(li);
}
add.addEventListener('click',e=>{
    addListItem('');
});
sort.addEventListener('click', e=>{
    const arr=new Array();
    const items= document.querySelectorAll('.list li');
    items.forEach(li=>{
        arr.push(li.firstElementChild.value);
    });
    arr.sort();
    if(sorted) {
        arr.reverse();
        e.target.classList.remove('yuxari');
        e.target.classList.add('asagi');
    }else{
        e.target.classList.remove('asagi');
        e.target.classList.add('yuxari');
    }
    sorted= !sorted;
    list.innerHTML='';
    arr.forEach(item=>{
        addListItem(item);
    });
});