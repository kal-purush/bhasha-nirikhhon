import express from 'express'
import {ProductManager} from "./ProductManager.js"


const app = express()

const prod = new ProductManager('./ProductManager.json')

app.get('/products/:pid',async(request,response)=>{

    const prod2 = await prod.getProducts();
    const id =  request.params.pid
    const product = await prod2.find(product=>product.id==id)
    
    if(!product){
    response.send({error: 'el ID solicitado no existe'})
    }else{

        response.send(product)
    }

})

app.get('/products',async(request,response)=>{
    const prod2 = await prod.getProducts();
    const limite = request.query.limite
    const product = await prod2.slice(0,limite)
   if(!limite) {
    return response.send(prod2)
    
}else{
    return response.send(product)
}

})

app.listen(8080,()=>console.log("Server running"))