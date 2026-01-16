const db = require('../models/connection')
const Image = db.image
const fs = require('fs');


const uploadImage=async(req,res,next)=>{
    const newImage = await Image.create({url:req.file.path})
    res.json({
        message:"Uploaded Image"
    })
}

const showImage = async(req,res,next)=>{
    const id = req.params.id;
    let image;
    try{
        image = await Image.findByPk(id);
    }catch(e){

    }
    fs.readFile(image.url,(err,data)=>{
        if(err){
            const error = new Error(e);error.code=400;return next(error)
        }else{
            res.end(data)
        }
    })
    
}


module.exports = {uploadImage,showImage}