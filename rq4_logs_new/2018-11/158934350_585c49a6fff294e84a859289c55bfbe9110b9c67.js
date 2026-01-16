var config=require('../config');
var db=require('../models/index');
var async=require('async');

module.exports.create=function(req,res){

    var title=req.body.title;
    var content=req.body.content;
    var userId=req.body.userId;
    // var likes=req.body.likes;

    async.waterfall([function(done) {
        db.User.findOne({where:{userId:userId}}).then((obj) => {

            if(obj == null || obj == undefined){
                return res.send({status:0,message:'User Id not Found'});
            } else {
                done(null,obj)
            }
        })
    },function(user,done){

        db.Feed.create({title:title,content:content,userId:userId}).then((obj) => {

            if(obj == null){
                res.send({status:0,message:'Something went wrong'});
            } else {
                res.status(200).send({status:1,message:'News created successfully'});
            }
        })
    }]);

}

module.exports.update=function(req,res){

    var title=req.body.title;
    var content=req.body.content;
    var userId=req.body.userId;
    var id=req.params.id;
    const Op=db.Sequelize.Op;
    async.waterfall([function() {
        db.Feed.findOne({where:{id:id,[Op.and]:{is_deleted:0}}}).then((obj) => {

            if(obj == null){
                return res.send({status:0,message:'News Id not Found'});
            } else {
               
                return done(null,obj);
            }
        })
    },function(news,done) {
        db.User.findOne({where:{userId:userId}}).then((obj) => {

            if(obj == null || obj == undefined){
                return res.send({status:0,message:'User Id not Found'});
            } else {
                return done(null,obj)
            }
        })
    },function(user,done){

        db.Feed.update({title:title,content:content,userId:userId,where:{id:id}}).then((obj) => {

            if(obj == null){
                res.send({status:0,message:'Something went wrong'});
            } else {
                res.status(200).send({status:1,message:'News created successfully'});
            }
        })
    }]);
}

module.exports.postedlists=function(req,res) {


      var id=req.params.id;
      const Op=db.Sequelize.Op;
    async.waterfall([function() {
        db.Feed.findAll({where:{is_deleted:0,[Op.and]:{user_id:id}}}).then((obj) => {
        
            res.send(obj);
        })
    }])
}

module.exports.view=function(req,res) {

    var id=req.params.id;
    const Op=db.Sequelize.Op;
    async.waterfall([function(done) {

        db.Feed.findOne({where:{id:id,[Op.and]:{is_deleted:0}}}).then(obj => {

            res.send(obj);
        })
    }])

module.exports.delete=function(req,res) {

    var id=req.params.id;

    async.waterfall([function(done) {

        db.Feed.findOne({where:{id:id}}).then((obj) => {
            
            if(obj != null) {

                obj.updateAttributes({is_deleted:1}).then((obj) => {
                   
                    res.status(200).send({status:1,message:'deleted successfully'});
            
                })
            }else {
                res.send({status:0,message:'News id not found'});
            }
        })
        
    }])
}


}

module.exports.newslists=function(req,res) {


    var id=req.params.id;
    const Op=db.Sequelize.Op;
  async.waterfall([function() {
      db.Feed.findAll({where:{is_deleted:0,[Op.not]:{user_id:id}}}).then((obj) => {
      
          res.send(obj);
      })
  }])
}