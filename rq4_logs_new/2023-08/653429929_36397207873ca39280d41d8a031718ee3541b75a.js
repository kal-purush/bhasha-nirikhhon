const express = require("express");
const users = express.Router();
const { getAllUsers, createUser } = require("../queries/users");


// Index
users.get("/", async (req, res) => {

    try {
        const allUsers = await getAllUsers();
        res.status(200).json(allUsers);
    } catch(err) {
        res.status(500).json({ error: err.message });
    }

});

// Create
users.post("/", async (req, res) => {
    console.log(req.body)
    const user = req.body;
    console.log(`userController Create method: ${JSON.stringify(user)}`)

    try {
        const newUser = await createUser(user);
        res.status(200).json(newUser);
    } catch (err) {
        res.status(400).json({ error: err.message });
    }
});


module.exports = users;