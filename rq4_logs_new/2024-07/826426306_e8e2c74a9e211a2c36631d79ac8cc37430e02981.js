const jwt = require('jsonwebtoken');

// Middleware to check if user is not authenticated
const checkNotAuth = (req, res, next) => {
    const { token } = req.cookies;

    if (token) {
        jwt.verify(token, 'pninfosystytytt', (err, decoded) => {
            if (err) {
                next(); // If there's an error, proceed to the login page
            } else {
                res.redirect('/home'); // If the token is valid, redirect to the home page
            }
        });
    } else {
        next(); // If there's no token, proceed to the login page
    }
};

module.exports = checkNotAuth;