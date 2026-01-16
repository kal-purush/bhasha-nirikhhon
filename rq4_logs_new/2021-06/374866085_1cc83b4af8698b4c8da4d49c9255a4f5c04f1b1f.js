const jwt = require('jsonwebtoken');

//VERIFYING JWT
module.exports = (req, res, next) => {
    const token = req.header("auth-token");
    if(!token) {
        console.log("************************&&&&&&&&&&&&&&&&&&&&&&&&");
        res.status(401).send("access denied."); // token == null
    }
    try {
        const verified = jwt.verify(token, process.env.TOKEN_SECRET); 
        req.user = verified;
        next();
    } catch (error) {
       res.status(400).send("invalid token"); 
    }
}



// Multiple Functions
// exports.login
// exports.signup

// Single Function to Export
// module.exports = () => {}