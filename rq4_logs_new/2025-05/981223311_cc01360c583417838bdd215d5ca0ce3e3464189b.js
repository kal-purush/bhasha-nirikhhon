import mongoose from "mongoose"; 


const connentDB = async () => {

    mongoose.connection.on('connected', () => console.log("database connected"));

    await mongoose.connect(`${process.env.MONGODB_URI}/Anjani_web`); // Use your url and project name
};

export default connentDB;