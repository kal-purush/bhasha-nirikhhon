const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');

const app = express();
const port = 5000;
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
})

app.use(cors());
app.use(bodyParser.json());

const SECRET_KEY = "adichi_kettalum_solladha";

app.post('/api/login', (req,res) => {
    const {email, password} = req.body;

    if(email === "nothing@gmail.com" && password === "652020") {
        const token = jwt.sign({email}, SECRET_KEY, {expiresIn: '1m'});
        return res.json({token});
    }

    return res.status(401).json({message : "Invalid Credentials"});
})

app.get('/api/dashboard', (req, res) => {
    const authHeader = req.headers.authorization;
    const token = authHeader?.split(" ")[1];
    if(!token){
        return res.status(401).json({message: "Unauthorized"});
    }

    jwt.verify(token, SECRET_KEY, (err, user) => {
        if(err) {
            return res.status(403).json({message: "Forbidden"});
        }
        return res.json({message: "Welcome to the dashboard", user});
    })
})