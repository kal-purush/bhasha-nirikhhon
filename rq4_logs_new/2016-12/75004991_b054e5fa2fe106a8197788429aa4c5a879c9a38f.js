
var mongoose = require('mongoose');
var QuestionsModel = require("./../models/questionsModel.js");

module.exports = {

  // read: function(req, res){
  //   QuestionsModel.find(req.query)
  //     .exec(function(err, result){
  //       if(err){
  //         res.send(err);
  //       }else{
  //         res.send(result);
  //       }
  //     });
  // },

  create: function(req, res){
    var Question = new QuestionsModel(req.body);
    Question.save(function(err, result){
      if(err){
        res.send(err);
      }else{
        res.send(result);
      }
    });
  },

  // update: function(req, res){
  //   QuestionModel.findByIdAndUpdate(req.params.id, req.body, function(err, result){
  //     if(err){
  //       res.send(err);
  //     }else{
  //       res.send(result);
  //     }
  //   });
  // },

  // delete: function(req, res){
  //   QuestionModel.findByIdAndRemove(req.params.id, req.body, function(err, result){
  //     if(err){
  //       res.send(err);
  //     }else{
  //       res.send(result);
  //     }
  //
  //   });







}//end of exports model