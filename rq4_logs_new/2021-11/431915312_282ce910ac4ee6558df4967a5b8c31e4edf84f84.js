// variable------------
   
   //modal
const backDrop=document.querySelector('#backdrop');  
const modalTasks=document.querySelector('.modal');
const addTodoBtn=document.querySelector('.add-todo');
const btnCancelModal=document.querySelector('.btn-cancel');
const btnOkModal=document.querySelector('.btn-ok');
const modalError=document.querySelector('.modal-error');
const closeModalBtn=document.querySelector('.close-modal');

let inputTitle=document.querySelector('#title-task');
let inputDis=document.querySelector('#task-dis');

    //todo
 const listTodo=document.querySelector('#list')   
  let emptyList=document.querySelector('.empty-list');
  let subListTodo=document.querySelector('#list ul') 
  let todoList=getLocal()
  
  const checkEmpty=()=>{
    if(todoList.length==0){
      emptyList.classList.remove('active')
    }
    else{
      emptyList.classList.add('active')
    }

  }

 const showModal=(modalName)=>{
    backDrop.classList.add('visible')
    modalName.classList.add('active')
    
  }
       //close modal
  const closeModal=()=>{
      backDrop.classList.remove('visible')
      modalTasks.classList.remove('active')
      modalError.classList.remove('active')
     
  }





  const filter= {
      searchItem:'',
      complete:false,
      sorts:'ByEdited'
   }
   renderProduct(todoList,filter)
 

   // add-task-array
   const addToArray=()=>{
    let id=uuidv4()
    const time=moment().valueOf()
    let title=inputTitle.value;
    let dis=inputDis.value;
    if(title==''&&dis===''){
       showModal(modalError)
       modalTasks.classList.remove('active')
    }
    else{
        todoList.push(
            {
                id:id,
                title:title,
                dis:dis,
                completd:true,
                created:time,
                updated:time
                
            }
            
        )
        saveLocal(todoList)
        checkEmpty()
        renderProduct(todoList,filter)
        inputTitle.value=''
        inputDis.value=''
        closeModal()
    }
    
   
}

// eventlistner-------------------

const eventListner=()=>{

  document.addEventListener('DOMContentLoaded',()=>{
    checkEmpty()
  })
 addTodoBtn.addEventListener('click',()=>{
    showModal(modalTasks)
 })

 backDrop.addEventListener('click',closeModal)

 btnCancelModal.addEventListener('click',closeModal)
 btnOkModal.addEventListener('click',addToArray)

 document.querySelector('#search-input').addEventListener('input',(e)=>{
     filter.searchItem=e.target.value.trim()
     renderProduct(todoList,filter)
    
 })

 document.querySelector('#show-by').addEventListener('change',(e)=>{
   filter.complete=e.target.checked
   renderProduct(todoList,filter)
   
 })
 document.querySelector('#sort').addEventListener('change',(e)=>{
   filter.sorts=e.target.value
   renderProduct(todoList,filter)
   
 })
  closeModalBtn.addEventListener('click',closeModal)
 window.addEventListener('storage',(e)=>{
   if(e.key==='task'){
     todoList=JSON.parse(e.newValue)
     renderProduct(todoList,filter)
   }
 })
}
eventListner()