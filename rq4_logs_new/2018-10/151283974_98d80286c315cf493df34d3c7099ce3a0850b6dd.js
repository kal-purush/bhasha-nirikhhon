const express = require('express');
const passport = require('../passport');
const jwt = require('jsonwebtoken');
const router = express.Router();
const cache = require('../../Cache/Cache');

/*
{
    success: true,    
    user: {
        id,
        username,
        email,
    },
    token: token,
    hasOAuth: true/false,
}
OR
{
    success: false,
    message
}
*/
router.post('/',(req,res)=>{
    passport.authenticate('local',{session:false},(err,user,info)=>{
        if(user){
            jwt.sign({user:user.id}, process.env.JWT_SECRET, (err,token)=>{
                cache.getAllOAuthTokens(user.id).then(result=>{
                    res.json({
                        success:true,
                        user:user,
                        token,
                        hasOAuth:true,
                        accounts:result,
                    });
                    res.end();
                }).catch(err=>{
                    res.json({
                        success:true,
                        user:user,
                        token,
                        hasOAuth:false,
                    });
                    res.end();
                })
            })
        }else{
            res.json({
                success:false,
                message:info.message
            });
            res.end();
        }
    })(req,res);
})

module.exports = router;