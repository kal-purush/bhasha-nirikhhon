const express = require("express");
const app = express();

app.use(allbooks)

app.get("/book", function(req,res) {

  return res.send({
      maths:"Mathematics",
      eng :"English"
  })
})

app.get("/book_to_eat",  singlebook,function(req,res) {

    return res.json({
        must_read:"Eat That bloody frog"
    })
  })
  
  app.get("/book_to_admire",  singlebook,function(req,res) {

    return res.json({
        must_read:"The subtle art to not of giving a fuck"
    })
  })


app.listen(7000,()=>{
    console.log("I am Listening on port 7000")
})

function allbooks(req,res,next){
console.log("Fetching all Books")
next()

}


function singlebook(req,res,next){
if( req.role == "Eat That Frog"){
 req.path = "/book_to_eat"
}else if(req.role == "The subtle art of not giving a fuck"){
    req.path = "/bokk_to_amdire"
}
next()

}