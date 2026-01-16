const express = require('express');
const Biketrail = require("../models/biketrail");
const Comment = require("../models/comment");
const Image = require("../models/image");


let middleware = {};

// middleware to test if user is logged in otherwise he can go to secret page via search line
middleware.isLoggedIn = (req,res,next) => {
    console.log("isLoggedIn called!");
    if(req.isAuthenticated()){
        console.log("user " + req.body.username + " is authenicated!")
        return next();
    }
    res.redirect("/login");
};

// check Biketrail and Image Ownership
middleware.checkBiketrailOwnership = (req,res,next) => {
    console.log("check biketrailOwnership called!");
    // console.log("Show req object:",req);
    if(req.isAuthenticated()){
        Biketrail.findById(req.params.id,(err,foundBiketrail) => {
            console.log("Found Biketrail in middleware.checkBiketrailOwnership:",foundBiketrail);
            if(err){
                console.log("Error in middleware.checkBiketrailOwnership",err);
            } else {
                const bikeTrailOwner_id = foundBiketrail.author.id;
                const user_id = req.user._id;
                console.log("biketrailOwner_id: ",bikeTrailOwner_id);
                console.log("user_id: ",user_id);
                if(bikeTrailOwner_id !== undefined && bikeTrailOwner_id.equals(user_id)){
                    console.log("Biketrail Owner okay!");
                    next();
                }
                else {
                    console.log("You are not the bikeTrailOwner and are not allowed to do that!");
                    res.redirect("back");
                }
            }
        });
    } else {
        res.redirect("/login");
    }
};

// check Comment Ownership
middleware.checkCommentOwnership = (req,res,next) => {
    console.log("check commentOwnership called!");
    // console.log("Show req object:",req);
    if(req.isAuthenticated()){
        Comment.findById(req.params.comment_id,(err,foundComment) => {
            console.log("Found Commment in middleware.checkCommentOwnership:",foundComment);
            if(err){
                console.log("Error in middleware.checkCommentOwnership",err);
            } else {
                const commentOwner_id = foundComment.author.id;
                const user_id = req.user._id;
                console.log("commentOwner_id: ",commentOwner_id);
                console.log("user_id: ",user_id);
                if(commentOwner_id !== undefined && commentOwner_id.equals(user_id)){
                    console.log("Comment Owner okay!");
                    next();
                }
                else {
                    console.log("You are not the CommentOwner and are not allowed to do that!");
                    res.redirect("back");
                }
            }
        });
    } else {
        res.redirect("/login");
    }
};

module.exports = middleware;