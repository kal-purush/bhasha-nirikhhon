var express = require("express");
var app = express();
app.set("view engine", "ejs");
app.set("views", "./views");
app.use(express.static("public"));
app.listen(3000);
var mongoose = require('mongoose');
var bodyParser = require('body-parser')
var urlencodedParser = bodyParser.urlencoded({ extended: false });
var multer  = require('multer');
const bcrypt = require('bcrypt')
const saltRounds = 10;
const jwt = require('jsonwebtoken')
const serect = "X>0??Hn@tNIm9aM}Ahx#7b4";

mongoose.connect("mongodb+srv://root:ACyq5EvTI708XTJl@cluster0.xzxqoxu.mongodb.net/library?retryWrites=true&w=majority", {
    useCreateIndex: true,
    useFindAndModify: false,
    useNewUrlParser: true,
    useUnifiedTopology: true
}, err =>{
    if(err) throw err;
    console.log('Connected to MongoDB')
})

//module
const Book = require("./models/book");
const Category = require("./models/category");
const User = require("./models/user");

var storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, "public/upload")
    },
    filename: (req, file, cb) => {
        cb(null, file.originalname)
    }
})

var session = require("express-session");
app.use(session({
    secret: "AbnUExJPTiAEmtlcFmOC3eyUjF7kN",
    cookie: {maxAge: 6600000000}
}))

var upload = multer({
    storage: storage,
    fileFilter: (req, file, cb) => {
        if (file.mimetype == "image/bmp" || 
        file.mimetype == "image/png" || 
        file.mimetype == "image/gif" ||
        file.mimetype == "image/jpg" ||
        file.mimetype == "image/jpeg"
        ) {
            cb(null, true)
            
        } else {
            return cb(new Error("Only image are allowed!"))
        }
        
    }
}).single("image")

function checkToken(req, res) {
    if(req.session.token) {
        jwt.verify(req.session.token, serect, (err, decoded) => {
            if(err) {
                res.redirect("http://localhost:3000/login")
            }
        })
    } else {
        res.redirect("http://localhost:3000/login")
    }
}

app.get("/page/book", (req, res) => {
    checkToken(req, res)
    Category.find( (err, items) => {
        if(err) {
            res.render("book", {page:"book"});
        } else {
            res.render("book", {page:"book", cats:items});
        }
    })
})

app.post("/page/book", urlencodedParser, (req, res) => {
    checkToken(req, res)
    upload(req, res, (err) => {
        if(err instanceof multer.MulterError) {
            console.log("A Multer error occurred when uploading." + err)
        }else if(err) {
            console.log("A Multer error occurred when uploading." + err)
        } else {
            var book = new Book({
                title: req.body.title,
                author: req.body.author,
                description: req.body.description,
                publishDate: req.body.created_at,
                pageCount: req.body.page_count,
                ImageUrl: req.file.originalname
            });
            book.save( (err) => {
                if (err) {
                    res.json({kq: 0, "err": err})
                } else {
                    category.findOneAndUpdate({_id: req.body.selectCate}, {$push: {books_id: book._id}}, (err) => {
                        if (err) {
                            res.json({kq:0, "err": err})
                        } else {
                            console.log("book is ok");
                            res.redirect("/")
                        }
                    })
                }
            })
        }
    });
})
app.post("/page/category", urlencodedParser, (req, res) => {
    checkToken(req, res)
    var cate = new Category({
        title: req.body.title,
        ordering: req.body.ordering,
        books_id: []
    })

    cate.save( (err) => {
        if (err) {
            res.render("category", {page: "category", message: "save category errors"})
        } else {
            res.render("category", {page: "category", message: "save category success"})
        }
    })
})


app.get("/page/category", (req, res) => {
    res.render("category", {page:"category"});
})

app.get("/", (req, res) => {
    Book.find( (err, items) => {
        if (err) {
            console.log(err);
            return null;
        } else {
            res.render("home", {page:"home", books:items});
        }
    })
})

app.get("/login", (req, res) => {
    res.render("login");
})

app.post("/login", urlencodedParser, (req, res) => {
    User.find({email:req.body.email}, (err, item) => {
        if (!err && item.length > 0) {
            bcrypt.compare(req.body.password, item[0].password, (err2, res2) => {
                if(err2) {
                    res.json({kq:0, err: "Wrong pass"});
                }else {
                    jwt.sign(item[0].toJSON(), serect, {expiresIn: "168h"}, (err, token) => {
                        if(err) {
                            res.json({kq:0, err: "wrong token:" + err})
                        } else {
                            req.session.token = token;
                            res.redirect("http://localhost:3000/")
                        }
                    });
                }
            })
        } else {
            res.json({kq:0, err: "Wrong email"});
        }
    })
})

app.get("/register", (req, res) => {
    res.render("register");
})

app.post("/register", urlencodedParser, (req, res) => {
    bcrypt.hash(req.body.password, saltRounds, (err, has) => {
        let user = new User({
            name: req.body.name,
            email: req.body.email,
            password: has
        });
        user.save( (err) => {
            if (err) {
                res.json({kq:0})
            } else {
                res.redirect("/login")
            }
        })
    })
    
})

app.get("/book/:id", (req, res) => {
    Book.find( {_id:req.params.id},(err, item) => {
        if (err) {
            console.log(err);
            return null;
        } else {
            res.render("bookDetail", {page:"bookDetail", book:item});
        }
    })
})

