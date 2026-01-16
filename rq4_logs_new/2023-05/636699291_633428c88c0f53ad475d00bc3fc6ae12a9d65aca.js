var cookieParser = require("cookie-parser");
var session = require('express-session');
var createError = require("http-errors");

var passport = require('passport');
var express = require("express");

var logger = require("morgan");
var cors = require('cors');
var path = require("path");

var JwtStrategy = require('passport-jwt').Strategy;

var bicicletasRouter = require("./routes/bicicleta");
var bicicletasApiRouter = require("./routes/api/bicicleta");
ExtractJwt = require('passport-jwt').ExtractJwt;

var app = express();

app.use(cors({ credentials: true, origin: true }));

// view engine setup
app.set("views", path.join(__dirname, "views"));
app.set("view engine", "pug");

app.use(logger("dev"));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.use(cookieParser());
app.use(express.static(path.join(__dirname, "public")));

app.use(session({
    secret: process.env.SECRET,
    resave: false,
    saveUninitialized: false
}));

app.use(passport.initialize());
app.use(passport.session());


var TOKEN_SECRET = process.env.SECRET;

var cookieExtractor = function(req) {
    let token = null;
    if (req && req.cookies)
    {
      token = req.cookies['auth'];
    }
    return token;
};


var opts = {
jwtFromRequest: ExtractJwt.fromExtractors([cookieExtractor]),
secretOrKey: TOKEN_SECRET,
};
passport.use(
'jwt',
new JwtStrategy(opts, (jwt_payload, done) => {
    try {
    console.log('jwt_payload', jwt_payload);
    done(null, jwt_payload);
    } catch (err) {
    done(err);
    }
}),
);

app.use("/", passport.authenticate('jwt', { session: false }), bicicletasRouter);
app.use("/api", passport.authenticate('jwt', { session: false }), bicicletasApiRouter);
// catch 404 and forward to error handler
app.use(function(req, res, next) {
    next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
    // set locals, only providing error in development
    res.locals.message = err.message;
    res.locals.error = req.app.get("env") === "development" ? err : {};

    // render the error page
    res.status(err.status || 500);
    res.render("error");
});

module.exports = app;