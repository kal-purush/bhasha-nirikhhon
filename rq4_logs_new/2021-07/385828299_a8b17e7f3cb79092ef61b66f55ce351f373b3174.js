const express = require('express');
const path=require('path');
const app= express();
const port=4000;
// const body = require("body-parser");
//body-parser helps in saving the body after parsing of post request in database
const mongoose = require('mongoose');
mongoose.connect('mongodb://localhost:27017/ContactDance', {useNewUrlParser: true, useUnifiedTopology: true});

//Define mongoose schema
const contactSchema = new mongoose.Schema({
    name: String,
    phone: String,
    email: String,
    address: String,
    desc: String,
  });

const contact = mongoose.model('contact', contactSchema);

//EXPRESS RELATED STUFF 
app.use('/static', express.static(path.join(__dirname,'static')));  // FOR SERVING STATIC FILES
app.use(express.urlencoded());

//PUG RELATED STUFF
app.set('view engine','pug');  //SET THE TEMPLATE ENGINE AS PUG
app.set('views',path.join(__dirname,'views'));  //SET THE VIEWS DIRECTORY

//ENDPOINTS
app.get('/',(req,res)=>{
    //if we want to add variables to the pug file we can do this
    const params = {}
    res.status(200).render('index.pug',params);
})
app.get('/home',(req,res)=>{
    //if we want to add variables to the pug file we can do this
    const params = {}
    res.status(200).render('home.pug',params);
})
app.get('/contact',(req,res)=>{
    //if we want to add variables to the pug file we can do this
    const params = {}
    res.status(200).render('contact.pug',params);
})

app.post('/contact',(req,res)=>{
    var myData = new contact(req.body);
    myData.save().then(()=>{
        res.send("This item has been saved to database")
    }).catch(()=>{
        res.status(400).send("Item couldn't be saved")
    });

})


//START THE SERVER
app.listen(port,()=>{
    console.log(`The application is successfully started at port ${port}`);
})