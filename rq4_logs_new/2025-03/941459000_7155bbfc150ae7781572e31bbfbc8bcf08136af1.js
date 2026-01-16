//Description: Node.js HTML client
//request: npm install express ejs axios body-parser

// const express = require('express');
// const axios = require('axios');
// const app = express();
// var bodyParser = require('body-parser');
// const path = require("path");
const express = require('express');
const bcrypt = require('bcrypt');
const Users = require('./path/to/your/models/User'); // ตรวจสอบให้แน่ใจว่า path ถูกต้อง
const app = express();

//Base URL for the API
//const base_url = "https://api.example.com";
const base_url = "http://localhost:3000";
app.set("views", path.join(__dirname, "/public/views"));
app.set("view engine", "ejs");

//Set the template engine
app.set("views" , path.join(__dirname, "/public/views"));
app.set('view engine', 'ejs');
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false}));

//Serve static files
app.use(express.static(__dirname + '/public'));

app.get("/", async (req, res) => {
    try{
        const response = await axios.get(base_url + '/users');
        res.render("head", { users: response.data });
    }catch(err){
        console.error(err);
        res.status(500).send('Error');
    }
});


app.get("/login", (req, res) => {
    res.render("login");  
});

app.get("/users/:id", async (req, res) => {
    try{
        const response = await axios.get(base_url + '/users/' + req.params.id);
        res.render("login", { book: response.data });
    }catch(err){
        console.error(err);
        res.status(500).send('Error');
    }
});
app.get("/users/:id", async (req, res) => {
    try{
        const response = await axios.get(base_url + '/users/' + req.params.id);
        res.render("register", { book: response.data });
    }catch(err){
        console.error(err);
        res.status(500).send('Error');
    }
});
app.get("/users/:id", async (req, res) => {
    try{
        const response = await axios.get(base_url + '/users/' + req.params.id);
        res.render("cart", { book: response.data });
    }catch(err){
        console.error(err);
        res.status(500).send('Error');
    }
});

app.get("/users/:id", async (req, res) => {
    try{
        const response = await axios.get(base_url + '/users/' + req.params.id);
        res.render("rentalProcess", { book: response.data });
    }catch(err){
        console.error(err);
        res.status(500).send('Error');
    }
});


app.get("/create", (req, res) => {
    res.render("create");
});

// app.post("/create", async (req, res) => {
//     try{
//         const data = {username: req.body.username, password:req.body.password, email: req.body.email, phone: req.body.phone};
        
//         await axios.post(base_url + '/users', data);

//         res.redirect("/");
//     }catch(err){
//         console.error(err);
//         res.status(500).send('Error');
//     }
// });




// app.post("/login", async (req, res) => {
//     try{
//         const data = { email: req.body.email,password:req.body.password};
        
//         await axios.post(base_url + '/users', data);

//         res.redirect("/");
//     }catch(err){
//         console.error(err);
//         res.status(500).send('Error');
//     }
// });

app.get("/register", (req, res) => {
    res.render("register");  
});

// const Users = require('../models/User'); // ตรวจสอบว่า path ถูกต้อง


// Middleware สำหรับการจัดการข้อมูล JSON
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.post("/users", async (req, res) => {
    try {
        const { username, password, email, phone } = req.body;

        // ตรวจสอบข้อมูลที่ได้รับ
        if (!username || !password || !email || !phone) {
            return res.status(400).send('กรุณากรอกข้อมูลให้ครบถ้วน');
        }

        // แฮชรหัสผ่านก่อนที่จะบันทึก
        const hashedPassword = await bcrypt.hash(password, 10);

        // สร้างผู้ใช้ใหม่
        const newUser  = await Users.create({
            username,
            password: hashedPassword,
            email,
            phone_number: phone
        });

        // เปลี่ยนไปที่หน้าเข้าสู่ระบบหลังจากลงทะเบียนสำเร็จ
        res.redirect("/login");
    } catch (err) {
        console.error("เกิดข้อผิดพลาดระหว่างการลงทะเบียน:", err);
        res.status(500).send('เกิดข้อผิดพลาดระหว่างการลงทะเบียน');
    }
});

// เริ่มเซิร์ฟเวอร์
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});


// app.get("/register", (req, res) => {
//     res.render("register");  
// });
app.get("/cart", (req, res) => {
    res.render("cart");  
});
app.get("/head", (req, res) => {
    res.render("head");  
});
app.get("/rentalProcess", (req, res) => {
    res.render("rentalProcess");  
});



// app.get("/update/:id", async (req, res) => {
//     try{
//         const response = await axios.get(
//         base_url + '/books/' + req.params.id);
//         res.render("update", { book: response.data });
//     }catch(err){
//         console.error(err);
//         res.status(500).send('Error');
//     }
// });

// app.post("/update/:id", async (req, res) => {
//     try{
//         const data = {title: req.body.title, author: req.body.author };
//         await axios.put(base_url + '/books/' + req.params.id, data);
//         res.redirect("/");
//     }catch(err){
//         console.error(err);
//         res.status(500).send('Error');
//     }
// });

// app.get("/delete/:id", async (req, res) => {
//     try{
//         await axios.delete(base_url + '/books/' + req.params.id);
//             res.redirect("/");
//     }catch(err){
//         console.error(err);
//         res.status(500).send('Error');
//     }
// });

app.listen(5000, () => {
    console.log('Server started on http://localhost:5000');
    });