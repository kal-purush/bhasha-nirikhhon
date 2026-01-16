const express=require('express');
const bodyParser=require('body-parser');
const app=express();
app.use(bodyParser.urlencoded({extended:true}));
app.use(express.static("public"));
app.set("view engine","ejs");

var items=[];

app.get('/',(req,res)=>{
var today=new Date();
var options={
    weekday:"long",
    day:"numeric",
    month:"long"
}


var day=today.toLocaleDateString("en-US",options);


res.render("list",{ kidofday:day, added: items});
});

app.post('/',(req,res)=>{
   item= req.body.newitem;
   items.push(item);
  res.redirect('/');
});
app.listen(5555,function(){
    console.log("server is runnning");
});