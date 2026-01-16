const Thoughts = require('../../models/thoughts')
const express = require('express')
const router = express.Router()


router.get('/getall', async (req, res) => {
    try {
        const result = await Thoughts.find()
        res.json(result)
    } catch (err) {
        res.status(500).json(err)
    }

})

//why does this not work in insomnia????? WTF
router.post('/add', async (req, res) => {
    console.log(req.body)
    // const newThought = await Thoughts.create()
    // res.json(newThought)
    res.end()
});

router.put('/update', async (req, res) => {

})

router.delete('/destory', async (req, res) => {

    try {
        const result = await Thoughts.deleteOne()
        res.json(result)
    } catch (err) {
        res.status(500).json(err)
    }
})

// use '.create' to create and save data to DB

// Thoughts.create({
//   thoughtText: `There are two types of people in this world: Those who like Neil Diamond, and those who don't. My ex-wife loves him.`,
//   createdAt: '',
//   username: `Bob's baby steps`
// })

//projections for email only
// Thoughts.find({username: 'Stacey'}, 'email').then(user => console.log(user))

//projection to leave out data... use the "-" in sub-catagory
// Thoughts.find()

// Thoughts.findById()

// Thoughts.findByIdAndDelete('6521fc829e9e89804d2e766d')
//   .then((user => console.log(user)))



module.exports = router;