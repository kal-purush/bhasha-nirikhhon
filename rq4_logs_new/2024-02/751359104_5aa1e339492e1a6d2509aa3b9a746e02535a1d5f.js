const express = require('express');
const router = new express.Router();
const Student = require("../models/students");

// create a new student


// using promise
/*app.post("/students", (req, res) => {

    console.log(req.body);
    const user = new Student(req.body);
    // to save inside database
    user.save().then(() => {
        res.status(201).send(user);
    }).catch((e) => {
        res.status(400).send(e);
    })
}) */



// Post using asynvc- await
router.post("/students", async (req, res) => {

    try {
        const user = new Student(req.body);
        const createUser = await user.save();
        res.status(201).send(createUser);
    } catch (e) {
        res.status(400).send(e);
    }
})


// Read the data of the registered students.
router.get("/students", async (req, res) => {

    try {
        const studentsData = await Student.find();
        res.send(studentsData);
    } catch (e) {
        res.send(e);
    }
})

// get the individual student data using id
router.get("/students/:id", async (req, res) => {

    try {
        const _id = req.params.id;
        const studentData = await Student.findById(_id);
        res.send(studentData);
    } catch (e) {
        res.send(e);
    }
})

// get the individual student data using name
// app.get("/students/name/:name", async (req, res) => {
//     try {
//         const name = req.params.name;
//         console.log(req.params.name);
//         const studentData1 = await Student.findOne({ name: name });

//         if (!studentData1) {
//             // If no student with the given name is found
//             return res.status(404).send('Student not found');
//         } else {
//             res.send(studentData1);
//         }
//     } catch (e) {
//         res.status(500).send(e.message);
//     }
// });

// get the individual student data using email
// app.get("/students/email/:email", async (req, res) => {
//     try {
//         const email = req.params.email;
//         console.log(req.params.email);
//         const studentData2 = await Student.findOne({ email: email });

//         if (!studentData2) {
//             // If no student with the given name is found
//             return res.status(404).send('Student not found');
//         } else {
//             res.send(studentData2);
//         }
//     } catch (e) {
//         res.status(500).send(e.message);
//     }
// });

// update the students by its id

router.patch("/students/:id", async (req, res) => {
    try {
        const _id = req.params.id;
        const updatestudentdata = await Student.findByIdAndUpdate(_id, req.body, {
            new: true // this will directly update without sending send button twice
        });
        res.send(updatestudentdata);
    } catch (e) {
        res.status(404).send(e);
    }
})

// delete the students by its id

router.delete("/students/:id", async (req, res) => {
    try {
        const id = req.params.id;
        const deletestudentdata = await Student.findByIdAndDelete(id);
        if (!req.params.id) {
            return req.status(404).send();
        }
        res.send(deletestudentdata);
    } catch (e) {
        res.status(500).send(e);
    }
})

module.exports = router;