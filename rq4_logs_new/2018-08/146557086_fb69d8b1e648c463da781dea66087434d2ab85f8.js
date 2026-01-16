


//const MongoClient=require('mongodb').MongoClient;
const {MongoClient,ObjectID}=require('mongodb');

MongoClient.connect('mongodb://localhost:27017',(error,client)=>{
  var db=client.db('databasename1');
if(error){
  return console.log('unable to connect');
}

console.log('connect to database');

//deleteOne
// db.collection('mytablename1').deleteOne({name:'testname'}).then((count)=>{
// console.log('-----------------------------------------------')
//   console.log('total records are',count);
//   console.log('_________________________')
// },(err)=>{
//   console.log('unable to count---');
// })

//deleteMany

// db.collection('mytablename1').deleteMany({name:'testname'}).then((count)=>{
// console.log('-----------------------------------------------')
//   console.log('total records are',count);
//   console.log('_________________________')
// },(err)=>{
//   console.log('unable to count---');
// })


//findOneAndDelete

db.collection('mytablename1').findOneAndDelete({name:'kumar'}).then((count)=>{
console.log('-----------------------------------------------')
  console.log('total records are',JSON.stringify(count));
  console.log('_________________________')
},(err)=>{
  console.log('unable to count----');
})


})