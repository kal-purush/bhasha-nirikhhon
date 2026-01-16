import React,{useEffect,useState} from 'react'
import SearchBar from '../components/SearchBar'
import './Main.css'
import axios from "axios"
import {connect} from 'react-redux'
import {ADD,DELETE} from '../redux/Actions'
import Button from 'react-bootstrap/Button';
import Modal from 'react-bootstrap/Modal';


function Main(props) {
  const [post, setPost] = useState([]);
  const [query,setQuery] = useState('')
  const [i,setI] = useState(9)
  let Access_Key ='spioDeStM2j0OuRbqraec6QGlMtkJg48VOokXnStf88'
  let baseURL = `https://api.unsplash.com/search/photos?page=1&per_page=20&query=${query}&client_id=${Access_Key}`

  


  useEffect(() => {
    axios.get(baseURL).then((response) => {
      props.addData({data:response?.data?.results})
      
    });
  }, [query,]);

 
  
  function Search (data){
      setQuery(data)
  }

  function add(){
    setI((pre)=>pre+3)
  }
  

  function sub(){
    setI((pre)=>pre-3)
  }


  return (
    <div className ='main'>
      <SearchBar Search={Search} />
      {
        props.data.posts&&(
          <>
          <div style ={{display:'flex',width:'70vw', justifyContent:'space-evenly', flexWrap:'wrap'}}>
        {
          props.data.posts.slice(0,i).map((img)=>{
            return(<>
            <img style={{height:'20vh',width:'20vw',marginRight:'1vw',marginTop:'5vh'}} src = {img.urls.small} /></> )
          })
        }


      </div>{
        props.data.posts.length!==0&&i<= props.data.posts.length&&(<button onClick={()=>{add()}} className='load'>Load more</button>)
      }
      {
         props.data.posts.length!==0&&i>= props.data.posts.length&&(<button onClick={()=>{sub()}} className='load'>Show Less</button>)
      }
          </>
        )
      }
    </div>
    
  )
}

const mapDispatchToProps = (dispatch)=>{
  return{
    deletePost:(id) =>{ 
      dispatch(DELETE(id))},

     addData:(data) =>{    
      dispatch(ADD(data))} , 
  }
 };

 const mapStateToProps = (state) =>{
  return{
    data:state
  }
}
export default connect(mapStateToProps,mapDispatchToProps)(Main) 