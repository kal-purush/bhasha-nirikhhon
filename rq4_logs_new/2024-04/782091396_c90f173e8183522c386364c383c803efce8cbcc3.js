const projects = require('../Models/projectSchema')

//add project logic
exports.addProject = async(req,res)=>{
    console.log("Inside the addProject method");
    const {title,language,github,livelink,overview} = req.body
    const projectImage = req.file.Filename
    const userId = req.payload
    console.log(title,language,github,livelink,overview,projectImage);
    console.log(userId);
    res.status(200).json("Add Project request successful")
    try{
        const existingProject = await projects.findOne({github})
        if(existingProject){
            res.status(404).json("Project already exists")
        }
        else{
            const newProject = new projects({title,language,github,livelink,overview,projectImage,userId})
            await newProject.save()
            res.status(200).json(newProject)
        }
    }
    catch(err){
        res.status(406).json({message:err.message});
    }
}

//Get all project details for a particular user
exports.getProject = async(req,res)=>{
    console.log("Inside the getProject method");
    const userId = req.payload
    try{
        const project = await projects.find({userId})
        if(project){
            res.status(200).json(project)
        }
        else{
            res.status(404).json("No project found")
        }
    }
    catch(err){
        res.status(406).json({message:err.message});
    }
}


//Get three project details for homepage

//get all project details 
exports.getAllProject = async(req,res)=>{
    console.log("Inside the getAllProject method");
    try{
        const project = await projects.find()
        if(project){
            res.status(200).json(project)
        }
        else{
            res.status(404).json("No project found")
        }
    }   
    catch(err){
        res.status(406).json({message:err.message});
    }
}