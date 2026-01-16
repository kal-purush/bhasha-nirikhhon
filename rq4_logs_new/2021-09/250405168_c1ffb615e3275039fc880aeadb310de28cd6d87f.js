const router = require('express').Router();
const store = require('../db/store');

// GET "/api/gameDatas" -> responds with all the data from the database
router.get('/gameDatas', (req, res) => { 
//code goes here
});

// POST "/api/gameDatas" -> Add data to the database
router.post('/gameDatas', (req, res)=> {
    //code goes here
});

// Delete "/api/gameDatas" -> deletes data from the database
router.delete('/gameDatas', (req, res)=> {
    //code goes here
});
// UPDATE "/api/gameDatas" -> change data from the database
router.update('/gameDatas', (req, res)=>{
    //code goes here
})

module.exports = router;