const express = require("express");
const path=require('path');
const app = express();
const port = 4000;
const https=require('https');
const apiKey="9d871ad2b9208dc3684541b72083256e";
const city="Nur-sultan";
const ownSite=`https://api.openweathermap.org/data/2.5/weather?q=${city}&appid=${apiKey}&units=metric`;
const fs = require('fs');

const bodyParser=require('body-parser');
const userRoute=require('./routes/userRoutes.js')
let urlencodedParser = bodyParser.urlencoded({ extended: false });
const dbConfig = require('./config/database.config.js');
const mongoose=require('mongoose');
const User = require('./models/user.js');
app.use('/', userRoute);
app.use(express.json());


mongoose.Promise=global.Promise;
mongoose.connect(dbConfig.url, { 
    useNewUrlParser: true
 }).then(() => { 
    console.log("База данных успешно подключена!!");     
}).catch(err => { 
    console.log('Не подключено', err); 
    process.exit(); 
});
app.use('/user', userRoute)

app.use('/css', express.static(__dirname + '/public'))
const ejs=require('ejs');
const { response } = require("express");
app.set('view engine', 'ejs');
app.use("/public", express.static(__dirname + "/public"));
 
app.get('/',function (req,res){
    res.render('sign_in')
    
})

app.get('/main',function (req,res){
    https.get(ownSite, function(response){
        response.on("data", function(data){
            const weatherData = JSON.parse(data);
            const temperature=weatherData.main.temp;
            res.render('main',{temperature: temperature+"C"})
        });

   });
    
})

app.get('/about_css',function (req,res){
    https.get(ownSite, function(response){
        response.on("data", function(data){
            const weatherData = JSON.parse(data);
            const temperature=weatherData.main.temp;
            res.render('about_css',{temperature: temperature+"C"})
        });

   });
    
})

app.get('/about_js',function (req,res){
    https.get(ownSite, function(response){
        response.on("data", function(data){
            const weatherData = JSON.parse(data);
            const temperature=weatherData.main.temp;
            res.render('about_js',{temperature: temperature+"C"})
        });

   });
})
app.get('/about_html',function (req,res){
    https.get(ownSite, function(response){
        response.on("data", function(data){
            const weatherData = JSON.parse(data);
            const temperature=weatherData.main.temp;
            res.render('about_html',{temperature: temperature+"C"})
        });

   });
})
app.get('/sign_in',function (req,res){
    res.render('sign_in')
})
app.get('/sign_up',function (req,res){
    res.render('sign_up')
})
app.get('/create',function (req,res){
   res.render('create')
})
app.get('/update',function (req,res){
    res.render('update')
    
})
app.get('/delete',function (req,res){
    res.render('delete')
    
})
app.get('/results',function (req,res){
    res.render('results')
    
})
app.get('/find',function (req,res){
    res.render('find')
})
app.listen(port, () =>
    console.log(`App listening at http://localhost:${port}`)
);


