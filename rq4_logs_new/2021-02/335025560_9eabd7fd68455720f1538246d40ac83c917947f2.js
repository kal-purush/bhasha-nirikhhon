const express  = require("express");
const mongoose  = require("mongoose")
const app = express();


app.use(express.urlencoded({extended: true}));
mongoose.connect("mongodb://localhost/BugTrackingApplication",{useNewUrlParser: true,useUnifiedTopology: true})
.then(()=>console.log("database connected"))
.catch(console.log)

const UserSchema = new mongoose.Schema({
    username: String,
    password: String,
    bounty: Number,
    bugsreported: [String]
})
const BugSchema = new mongoose.Schema({
    bugdescription: {
        type: String,
        required: true
    },
    userid: String,
    status:{
        type: String,
        enum: ["new","assigned","open","rejected","fixed"],
        default: "new"
    }
})
const Bug  = mongoose.model("Bug",BugSchema);
const User  = mongoose.model("User",UserSchema);
app.get("/",(req, res)=>{
    res.render("index.ejs");
})
app.get("/user/new", (req, res)=>{
    res.render("signup.ejs");
})
app.post("/user/new", (req, res)=>{
    new User(req.body).save();
    res.redirect("/user");
})
app.get("/user", (req, res)=>{ 
    res.render("login.ejs")
})
app.post("/user",async (req,res)=>{
    const user = await User.findOne(req.body);
    // console.log(user._id);
    if(user !== null){
        // res.render("user/index.ejs",{user});
        res.redirect(`/user/${user._id}`);
    }else{
        res.redirect("/user");
    }
})
app.get("/user/:id",async (req,res)=>{
    const user = await User.findById(req.params.id);
    res.render("user/index.ejs",{user});
})

app.get("/bug/new/:id",async(req,res)=>{
    const user = await User.findById(req.params.id);
    res.render("bug/new.ejs",{user});
})
app.post("/bug/new/:id",async (req,res)=>{
    const id = req.params.id;
    const user = await User.findById(id);
    const {bugdescription} = req.body;
    const bug = new Bug({bugdescription: bugdescription,userid: user._id.toString()});
    bug.save();
    user.bugsreported.push(bug._id.toString());
    user.save();
    res.redirect(`/user/${id}`);
})


app.listen(3000,()=>{
    console.log("server started");
})