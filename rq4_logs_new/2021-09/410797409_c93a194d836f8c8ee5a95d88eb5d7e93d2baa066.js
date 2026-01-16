var express = require("express");

var router = express.Router();

const credential = {
    email:"admin@gmail.com",
    password:"admin123"
}
//login user


router.post('/login',(req,res)=>{
    if(req.body.email == credential.email && req.body.password==credential.password){
        req.session.user = req.body.email;
        res.redirect('/route/dashboard');
        // res.send("Login Successful");
    }
    else{
        // res.redirect('/route/loginFailed');
        res.send("Invalid UserName or Password")
    }
})

router.get('/dashboard',(req,res)=>{
    if(req.session.user){
        res.render('dashboard',{user:req.session.user})
    }
    else {
        res.send("unauthorized user")
    }
})
router.get('/logout',(req,res)=>{
   req.session.destroy((err)=>{
       if(err){
       res.send("error");
       }
       else{
           res.render('base',{title:"Express" , logout:"logout successfull...!" })
       }

   })

})

module.exports = router;