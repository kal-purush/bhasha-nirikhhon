//import mongoose from "mongoose";
import { SavedCracker } from "../models/SavedCracker.js";
import { Product } from "../models/cracker.js";


export const addNewCracker = async(req,res) =>{
    const {Pdname, price, images, Stock} = req.body
   try{
   const newlycreatedCraker = await Product.create({
    Pdname, price, images, Stock, user: req.user })
   res.json({message:"Cracker Created Successfully..!",newlycreatedCraker})

}

   catch(e){
    res.json({message:e})
    console.log(e)
   }}
   
   export const getCracker  = async (req,res) =>{
   const cracker = await Product.find()
   res.json({cracker})
 }

 export const getCrackerById = async (req,res)=>{
    const id = req.params.id
    try { 
        let cracker = await Product.findById(id)

        if(!cracker) res.json({message:'cracker not exist'})

        res.json({message:"Cracker by id", cracker})
        
    } catch (error) {
        res.json({message:error})
    }
}

export const getCrackerByUserId = async (req,res) =>{
    const userId = req.params.id;
    try {
      let cracker = await Product.find({user:userId});
   
      if (!cracker) res.json({ message: "Cracker not exist" });
   
      res.json({ message: "cracker by userId", cracker });
    } catch (error) {
      res.json({ message: error });
    }
   }
   
   export const saveCrackerById = async (req,res) =>{
    const id = req.params.id

    let cracker = await SavedCracker.findOne({cracker:id})

    if(cracker) return res.json({message:"cracker already saved"})

    cracker = await SavedCracker.create({cracker:id})
    
    res.json({message:"cracker saved Successfully..!"})
}

export const getSavedCracker  = async (req,res) =>{
    const cracker = await SavedCracker.find()
    res.json({cracker})
}
export default {getCracker, getCrackerById,getCrackerByUserId,saveCrackerById, getSavedCracker, addNewCracker};
    