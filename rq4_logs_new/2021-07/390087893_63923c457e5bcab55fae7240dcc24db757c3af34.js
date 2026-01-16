const router = require("express").Router();
const {Owner} = require("../../public/js");

// Create Route to owner post
router.post("/owner", async (req, res) => {
  try {
    const ownerData = await Owner.create({
      propertyAddress: req.body.propertyAddress,
      leaseLength: req.body.leaseLength,
      
    });

    req.session.save(() => {
      req.session.Owner = true;

      res.status(200).json(ownerData);
    });
  } catch (err) {
    console.log(err);
    res.status(500).json(err);
  }
});