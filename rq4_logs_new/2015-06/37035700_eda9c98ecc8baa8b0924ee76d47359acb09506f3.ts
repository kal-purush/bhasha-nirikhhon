import mongodb = require('mongodb');
import autoIncrement = require("mongodb-autoincrement");
import plane = require('./plane');

var server = new mongodb.Server('localhost', 27017, {auto_reconnect: true});
var db = new mongodb.Db('avionmake', server, { w: 1 });
db.open(function() {
	return true;
});

//TODO fix https://github.com/TheRoSS/mongodb-autoincrement/issues/2
export function getNextSequence(callback:(err, autoIndex)=>void){
    autoIncrement.getNextSequence(db, 'plane', null,  callback);
}

export function savePlane(plane:IPlane, callback:(err, res)=>void){
    db.collection('plane')
    .update({_id: plane._id}, plane, {upsert:true}, callback)
}


export function getPlane(id, fullPlane:boolean, callback:(err, res)=>void){
    //info are protected
    db.collection('plane').findOne({_id: Number(id)}, {info:0}, (err, p) => {
        if(fullPlane){
            p = plane.expandPlane(p);
        }
        callback(err, p);   
    });
}