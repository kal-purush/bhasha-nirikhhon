const router = require("express").Router();
const { classesModel } = require("../database/Models");
router.get("/getcourse", async (req, res) => {
    try {
        const course = await classesModel.find({}).exec();
        res.json({ course });
    } catch (e) {
        console.log(e);
    }
})
module.exports = router;