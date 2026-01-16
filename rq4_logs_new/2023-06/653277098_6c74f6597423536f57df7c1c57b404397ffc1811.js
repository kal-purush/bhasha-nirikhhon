const Workout = require('../models/Workout');
const mongoose = require('mongoose');

//Get All Workouts
const getAllWorkouts = async(req,res)=>{
    const workouts = await Workout.find({}).sort({createdAt: -1});
    res.status(200).json(workouts);
}

//Get a Single Workout
const getWorkout = async(req,res)=>{
    const {id} = req.params 

    if(!mongoose.Types.ObjectId.isValid(id)){
        return res.status(404).json({error:"No such Workout"})
    }

    const workout = await Workout.findById(id)

    if(!workout){
        return res.status(404).json({error:"No such Workout"})
    }

    res.status(200).json(workout)

}

//Create a Workout
const createWorkout = async(req,res)=>{
    try{
        const workout = await Workout.create(req.body)
        res.status(200).json(workout)
    }
    catch (error) {
        res.status(400).json({error:error.message})
    }
}

//Delete a Workout
const deleteWorkout = async (req,res)=>{
    const {id} = req.params 

    if(!mongoose.Types.ObjectId.isValid(id)){
        return res.status(404).json({error:"No such Workout"})
    }

    const workout = await Workout.findByIdAndDelete({_id:id})

    if(!workout){
        return res.status(404).json({error:"No such Workout"})
    }

    res.status(200).json(workout)
}

//Update a Workout
const updateWorkout = async(req,res)=>{
    const {id} = req.params 

    if(!mongoose.Types.ObjectId.isValid(id)){
        return res.status(404).json({error:"No such Workout"})
    }

    const workout = await Workout.findByIdAndUpdate({_id:id},{
        ...req.body
    })

    if(!workout){
        return res.status(404).json({error:"No such Workout"})
    }

    res.status(200).json(workout)
}


module.exports = {createWorkout,getAllWorkouts,getWorkout,deleteWorkout,updateWorkout}