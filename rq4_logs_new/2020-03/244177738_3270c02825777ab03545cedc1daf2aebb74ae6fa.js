import React,{useState,useEffect} from 'react'

const Artical = () => {
    const [artical,setArtical]=useState([])

    useEffect(()=>{
      fetch(`http://localhost:8000/Multiple`,{
          method:'GET',
          headers:{'Content-Type':'application/json'}
      })
      .then(res=>res.json(res))
      .then(data=>{
          console.log(data)
          setArtical(artical=> data)
      })
      .catch(err=>{
          console.log(err)
      })
    },[])
    return (
        <div className='mx-auto' style={{overflow:'scroll',width:'97%',height:'500px'}}>
        <div className='row d-flex'>
             {
                artical.map(items=>( 
                <div>  
                 <div className='col mx-2 card' style={{margin:'10px',width:'300px',height:'500px'}}>
                <div className='card-title bg-warning'>
                 <h1>{items.title}</h1>
                </div>
                <div className='card-image'>
                {/* { for(i=0;i< items.imagelocation.lenght){

                }
                } */}
                 <img src={items.imagelocation[0]} alt='pics' style={{height:'300px'}}/>
                 {/* {
                     items.imagelocation.map(pic=>(<div><img src={pic} style={{height:'200px'}} alt='pics'/></div>))
                 } */}
                 </div>
                <div className='card-body'>
                 {items.intro}
                </div>
                <div>
                    <button className='btn btn-primary'>See Artical</button>
                </div>
               </div>
            </div>
                
                ))
            }
           
        </div>
        </div>
    )
}

export default Artical