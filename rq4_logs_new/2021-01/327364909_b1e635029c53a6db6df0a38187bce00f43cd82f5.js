const express = require("express");
const app = express();
const mysql = require("mysql");

const db = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "Viole.27+",
    database: "CRUDDataBase"
});

app.get("/", (req, res) => {
    const sqlInsert = "INSERT INTO movie_reviews (movieName, movieReview) VALUES ('Inception','good movie');"
    db.query(sqlInsert, (err, result) => { 
        res.send("hellow Charlie");
    });
});
app.listen(3001, () => {
    console.log("express running on port 3001");
});