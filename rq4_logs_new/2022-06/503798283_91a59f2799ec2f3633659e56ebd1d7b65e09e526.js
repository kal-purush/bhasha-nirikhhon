var passwordValidator = require('password-validator');
const { schema } = require('../models/thing');

// Create a schema
var passwordSchema = new passwordValidator();

// Add properties to it
passwordSchema
.is().min(6)                                    // Minimum length 6
.is().max(30)                                  // Maximum length 30
.has().uppercase()                              // Must have uppercase letters
.has().lowercase()                              // Must have lowercase letters
.has().digits(2)                                // Must have at least 2 digits
.has().not().spaces()                           // Should not have spaces

module.exports = (req, res, next) => {
    if(passwordSchema.validate(req.body.password)){
        next();
    }
    else{
        return res
        .status(400)
        .json({error : "le mot de passe est invalide " + passwordSchema.validate('req.body.password', { list: true }) })
    }
}