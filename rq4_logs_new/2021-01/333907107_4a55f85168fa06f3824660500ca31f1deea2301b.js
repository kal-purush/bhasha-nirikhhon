

const add_qust=document.querySelector('.add-qust');
const qust_list=document.querySelector('.qust-list');
const btn_add_qust=document.querySelector('.btn-add-qust');
const btn_list_qust=document.querySelector('.btn-list-qust');


btn_list_qust.addEventListener('click', function(){
    btn_list_qust.classList.remove('btn-primary')
    btn_add_qust.classList.add('btn-primary')
    qust_list.classList.remove('hide')
    add_qust.classList.add('hide')
})
btn_add_qust.addEventListener('click', function(){
    btn_add_qust.classList.remove('btn-primary')
    btn_list_qust.classList.add('btn-primary')
    add_qust.classList.remove('hide')
    qust_list.classList.add('hide')
})


const qno=document.querySelector('.qno');
const qust=document.querySelector('.qust');
const mark=document.querySelector('.mark');
const add=document.querySelector('.add');
const ul=document.querySelector('.list-group');

add.addEventListener('click',function(){
   if(qno.value=== "" || qust.value ==="" || mark.value ===""){
       alert('Add inputs')
   }else{
//craete li
const li =document.createElement('li')
//add class
li.classList="list-group-item d-flex justify-content-between align-items-center list-qust mt-3"

//craete span
const span=document.createElement('span')
span.classList="list-q-no"
// add value
span.appendChild(document.createTextNode(qno.value))

//add span to li
li.appendChild(span)
//add value to li
li.appendChild(document.createTextNode(qust.value))

//craete second span
const span2=document.createElement('span')
span2.classList="badge badge-primary badge-pill list-mark"
span2.appendChild(document.createTextNode(mark.value))

//add span2 to li
li.appendChild(span2);

//add li to ul
ul.appendChild(li)

qno.value="";
qust.value="";
mark.value=""
   }
})

const clear=document.querySelector('.clear')
clear.addEventListener('click',function(){
    
    while(ul.firstChild){
        ul.removeChild(ul.firstChild)
    }

})

const submit=document.querySelector('.submit')
submit.addEventListener('click', function(){
    window.location.assign('admin.html')
})