const Support = require("../../db/schema/supportSchema");

const support = async (req, res) => {
  const { name, email, description } = req.body;
  
  try {
    const supportData = new Support({
      name,
      email,
      description
    })
   
    const response =  await supportData.save();
   
    if(response){
      res.status(200).json({
        success: true,
        msg: "Submitted Successfully"
      })
    } else {
      res.status(401).json({
        success: false,
        msg: "Uncaught Error"
      })
    }
  } catch (e) {
    res.status(500).json({
      success: false,
      msg: "Uncaught Error"
    })
  }
}

module.exports = {
  support
}