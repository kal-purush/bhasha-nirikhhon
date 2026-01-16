const express=require('express');
var router=express.Router();
const mongoose=require('mongoose');
const Event=mongoose.model('Event');


function updateRecord(req,res){
    Event.findOneAndUpdate({_id:req.body._id},req.body,{new:true},(err,doc)=>{
        if(!err){
            console.log("Update Success");
            res.redirect('events/list');
        }
        else{
            if(err.name=="ValidationError"){
                handleValidationError(err,req.body);
                res.render('events/addOrEdit',{
                    viewTitle:"Update Event",
                    event:req.body
                });
            }
            else{
                console.log("Error During Update: "+err);
            }
        }
    });
}

function handleValidationError(err,body){
    for (field in err.errors){
        switch(err.errors[field].path){
            case 'eventName':
                body['eventNameError']=err.errors[field].message;
                break;
            case 'date':
                body['dateError']=err.errors[field].message;
                break;
            case 'venue':
                body['venueError']=err.errors[field].message;
                break;
            default:
                break;
        }
    }
}

function insertRecord(req,res){
    var event=new Event();
    event.eventName=req.body.eventName;
    event.date=req.body.date;
    event.venue=req.body.venue;
    event.comments=req.body.comments;
    event.save((err,doc)=>{
        if(!err){
            console.log("Save Success");
            res.redirect('events/list');
        }
        else{            
            if(err.name=='ValidationError'){
                handleValidationError(err,req.body);
                res.render("event/addOrEdit",{
                    viewTitle:"Insert Event",
                    event:req.body
                })
            }
            else{
                console.log("Error during Recording: "+err);
            }
        }
    })
}

router.get('/',(req,res)=>{
    res.render("event/addOrEdit",{
        viewTitle:"Insert Event"
    });
});


router.get('/list',(req,res)=>{
    Event.find((err,docs)=>{
        if(!err){
            console.log();
            res.render("event/list",{
                list:docs
            });
        }
        else{
            console.log("Error in retrieving List: "+err);
        }
    });
});

router.get('/:id',(req,res)=>{
    Event.findById(req.params.id,(err,doc)=>{
        if(!err){
            res.render("event/addOrEdit",{
                viewTitle:"Update Event",
                event:doc
            });
        }
    });
});

router.post('/',(req,res)=>{
    console.log(req);
    if(req.body._id==""){
        insertRecord(req,res);
    }
    else{
        updateRecord(req,res);
    }
});

router.get('/delete/:id',(req,res)=>{
    Event.findByIdAndRemove(req.params.id,(err,doc)=>{
        if(!err){
            res.redirect('/events/list');
        }
        else{
            console.log("Error in removing Record: "+err)
        }
    });
});

module.exports=router;