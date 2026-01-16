

import { ConnectingAirportsOutlined } from '@mui/icons-material'
import { connectStorageEmulator } from 'firebase/storage'
import React, { useEffect } from 'react'
import { useState ,useContext} from 'react'
import { CartContext } from '../../contexts/cart.context'

const Cart = () => {
    const {viewDetail}=useContext(CartContext)
  
console.log("cart cart cart",viewDetail)







  





  return (
    <>
   <div className='max-w-screen-2xl mt-12'>
    <div className="w-[40%] bg-blue-400  p-5 mx-auto ">

        {
            
               
                    <div className="flex flex-col  justify-between items-center">
                            <div className="text-3xl p-5">cartName</div>
                        <div className="flex items-center">
                            {/* <img src={} className="w-50 h-50 rounded-md mr-5" alt=""/> */}
                        </div>
                     
                    </div>
                
            
        }

        
    </div>
    
   </div>
    </>
  )
}

export default Cart