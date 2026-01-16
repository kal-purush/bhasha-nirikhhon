const asyncHandler = require("express-async-handler");
const User = require("../models/userModel");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");
//@desc Register user
//@route Post /api/users/register
//Accept public
const registerUser =asyncHandler (async (req,res)=>{
   const {username,email,password} =req.body;
   if(!username || !email || !password){
    res.status(400);
    throw new Error("All fields are mandatory!")
   }
   const userAvaliable = await User.findOne({email});
   if(userAvaliable){
    res.status(400)
    throw new Error("User already registered!")
   }
   //Hash password section
   const hashedPassword = await bcrypt.hash(password,10);
   console.log("Hashed Password:",hashedPassword);
   const user = await User.create({
    username,
    email,
    password:hashedPassword
   });
   console.log(`User Created ${user}`);
   if(user){
    res.status(201).json({
        error:false,
        message:"Registration success",
        _id:user.id,
        // data:user,
        email:user.email,    
    })
   }else{
    res.status(400);
    throw new Error("User data is invalid!")
   }
    res.status(200).json({
        error:false,
        message:"Registration success",
        data:user
       
        }); 
   });
   //@desc Login user
//@route Post /api/users/login
//Accept public
const loginUser =asyncHandler (async (req,res)=>{
 
   const {email,password}=req.body;
   if(!email || !password){
    res.status(400);
    throw new Error("All fields are mandatory!");

   };
   const user = await User.findOne({email});
   //compare passward with hasedpassword
   if(user && (await bcrypt.compare(password, user.password))){
    const accessToken = jwt.sign({
        user:{
            username:user.username,
            email:user.email,
            id: user.id
        }
    },process.env.ACCESS_TOKEN_SECRET, 
    // {expiresIn : "24h"}
   
    );
   
    user.authToken = accessToken
    const saveUser = await user.save()
      
    res.status(200).json({
        
        data:saveUser
    });
   }else{
    res.status(401);
    throw new Error("email or password is invalid");
   }
    
   });
//@desc Current user
//@route Post /api/users/current
//Accept private
const currentUser =asyncHandler (async (req,res)=>{ 
    res.status(200).json({
        error:false,
        message:"Current user info",
        user:req.user 
        }); 
   });//End method


   //Index
   //Get All User Lists
   //@route Get /api/user/userlists
   const getUsers =asyncHandler (async (req,res)=>{
    const perPage = parseInt(req.query.perPage) || 10; 
    const currentPage = parseInt(req.query.page) || 1; 
    try {
    const totalCount = await User.countDocuments({delete_status:1});
    const totalPages = Math.ceil(totalCount / perPage);
    const user =await User.find({ delete_status: 1 })
    .skip((currentPage - 1) * perPage)
    .limit(perPage);
   
    res.status(200).json({
        error:false,
        message:"Author Lists",
        data:user,
        totalCount,
        currentPage,
        totalPages,  
        }); 

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'An error occurred' });
    }



   })//End method
   module.exports = {registerUser,getUsers,loginUser,currentUser};