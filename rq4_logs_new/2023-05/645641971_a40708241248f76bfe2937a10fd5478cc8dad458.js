const mongoose  = require("mongoose")

// module.exports = {
//     url: 'mongodb://127.0.0.1:27017/Uniproject'
//   }

  const connectiondb = async(dburl) =>{
    try {
        await mongoose.connect(dburl)
        console.log("Database created successfully")
        
    } catch (error) {
        console.log(error)
        
    }

  }
  module.exports = connectiondb