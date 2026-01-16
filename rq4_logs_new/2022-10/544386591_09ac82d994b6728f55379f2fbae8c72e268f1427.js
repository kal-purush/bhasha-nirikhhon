import { useEffect, useState } from 'react';
import './App.css';
import ToDoLists from './components/ToDoLists';
// import AddCircleIcon from '@mui/icons-material/AddCircle';
import Button from '@mui/material/Button';
import AddIcon from '@mui/icons-material/Add';



function App() {


  // const listitems=JSON.parse(localstorage.getItem('itemlist)||'[]')
  const getLocalItems=()=>{
    let listItem=localStorage.getItem('listsItems')
    if(listItem){
     return  JSON.parse(listItem)
    }else {
      return []
    } 
  
  }
const [lists,setNewLists]=useState(getLocalItems());
 const [addedText,setAddedText]=useState('');
const [displayRemove,setDisplayRemove]=useState('flex')
const[toggleSubmit,setToggleSubmit]=useState(false)
const[editItem,setEditItem]=useState(null)
const onChangeHandeler=(e)=>{
//  console.log(e.target.value)
 setAddedText(e.target.value)
}
const addListBelowHandeler=()=>{
   console.log('clicked')
   if(!addedText){
alert('Add text!!')
   }else if(addedText && toggleSubmit){
      setNewLists(
        lists.map((list)=>{
          if(list.id === editItem){
            return{...list,name:addedText}
          }
          return lists
        })
      
      )
      setAddedText('')
      setToggleSubmit(false)
      setEditItem(null)
   }

   else
  setNewLists((lists)=>{
    let allAddedText={id:new Date().getTime().toString() ,name:addedText}
  
    return [...lists,allAddedText]
  }) 
  setAddedText('')

}
const deleteListHandeler=(index)=>{
  console.log('deleted list')
   setNewLists((lists)=>{
    return lists.filter((list)=>{
   return   index!== list.id;    //index shouls not match is fill will return all remaining list
    })
   })
}




useEffect(()=>{
  localStorage.setItem('listsItems',JSON.stringify(lists))
},[lists])

const removeAllHandeler=()=>{
  setNewLists([])
  setDisplayRemove('none')
}

const editListHandeler=(id)=>{
   console.log(id)
   
   let newEditLists=lists.find((list)=>{
      return list.id===id
   })
   setToggleSubmit(true)
  //  setNewLists(newEditLists)
  setAddedText(newEditLists.name)
  setEditItem(id)
console.log(newEditLists.name)

}


  return (
    <div className='container'>
      <div className='container-wrapper'>
            <h2 className='heading'>To Do List</h2>
            <div className="input">
            <input type="text"value={addedText} onChange={onChangeHandeler}   />
            {
              !toggleSubmit?<Button size="small" className='btn-add' onClick={addListBelowHandeler} title='update-item' ><AddIcon/>Add </Button>:
              <Button size="small" className="btn edit"   onClick={addListBelowHandeler}    >
                <i className="bi bi-check2"    ></i>
              {/* <i className=" bi bi-pencil-square"></i> */}
               </Button>
            }
            
            </div>
              
            <div className='addLines '>
                       
                       {lists.map((list)=>{
                       return < ToDoLists list={list.name}  id={list.id} deleteList={deleteListHandeler} editList={editListHandeler} />
                       })

                       }
        
                       
              
            
            </div>
            {
              lists? '' :<div className="remove" style={{display:displayRemove }}>
               <Button size="small" className="btn btn-add remove-all" onClick={removeAllHandeler} >Remove All</Button>
               </div>
            }
          

      </div>




    </div>
    


  )
}

export default App;