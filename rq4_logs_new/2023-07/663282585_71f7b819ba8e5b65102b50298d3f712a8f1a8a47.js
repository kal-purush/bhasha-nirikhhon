const express = require('express')
const mongoose = require('mongoose')
const cors = require('cors')
const multer = require('multer')
const path = require('path')
const ProductModel = require('./src/models/Product')
const app = express()
app.use(cors())
app.use(express.json())

mongoose.connect('mongodb+srv://codewithgeo:angcuteko01234@e-commerce.9etdyjt.mongodb.net/e-commerce?retryWrites=true&w=majority')

const storage = multer.diskStorage({
    destination: (req,file,cb) =>{
    cb(null, 'public/images')
    },
    filename: (req,file,cb) => {
        cb(null,file.fieldname + "_" + Date.now() + path.extname(file.originalname) )
    }
    
})

const upload = multer({
    storage: storage
})

app.post('/upload',upload.single('file'), (req,res) =>{
    ProductModel.create({
        product_image: req.file.filename
    }).then(result => res.json(result)).catch(err=>console.log(err))

})

app.listen(3001, ()=>{
    console.log('Server is running.')
})