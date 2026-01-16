import express from "express";
import { getRepository } from "typeorm";
import { Student } from "../entities/student.entity";
import bcrypt from "bcrypt";
const router = express.Router();


//Register Route
router.post("/register", async (req, res) => {
    try {
        //Destructure req.body
        const {firstName, lastName, email, password} = req.body;
        //Check if user exists
        const studentRepository = await getRepository(Student);
        const user = await studentRepository.find({where : {email: email}});
        if(user.length !== 0 ){
            return res.status(401).send("User already exists");
        }
        //bcrypt users password
        const saltRound = 10;
        const salt = await bcrypt.genSalt(saltRound);
        const bcryptPassword = await bcrypt.hash(password, salt);
        //Enter new user inside student repository
        const newStudent = {firstName, lastName, email, password:bcryptPassword};
        const student = await studentRepository.create(newStudent);
        const results = await studentRepository.save(student);
        res.json(results);
    } catch (error) {
        console.error(error.message);
        res.status(500).send("Server Error");
    }
});


module.exports = router;