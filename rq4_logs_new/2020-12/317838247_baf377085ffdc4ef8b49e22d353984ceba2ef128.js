const express = require("express");
const BodyParser = require("body-parser");
require("./db/mongodb");

const Movies = require("./models/movies");

const app = express();

const port = process.env.PORT || 7001;

app.use(BodyParser.json());

app.post("/movies", async(req, res) => {
    try{
        const user = new Movies(req.body);
        const createUser = await user.save();
        res.status(201).send(createUser);
    }catch(e){
            res.status(400).send(e);
        }
})

app.get("/movies",async (req, res) =>{
    try{
        const MoviesData = await Movies.find();
        res.send(MoviesData);
    }catch(e){
        res.send(e);
    }
})

app.listen(port, () => {
    console.log("done 7001");
}) 