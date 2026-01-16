var cors = require('cors')
        , express = require('express')
        , app = express()
        , server = require('http').createServer(app)
        , passport = require('passport')
        , util = require('util')
        , bodyParser = require('body-parser')
        , cookieParser = require('cookie-parser')
        , session = require('express-session')
        , RedisStore = require('connect-redis')(session)
        , GoogleStrategy = require('passport-google-oauth2').Strategy;

app.use(cors());
app.use(function (req, res, next) {
    // Website you wish to allow to connect
    res.set('Access-Control-Allow-Origin', '*');
    // Request methods you wish to allow
    res.set('Access-Control-Allow-Methods', 'GET, POST, DELETE, PUT');
    // Request headers you wish to allow
    res.set('Access-Control-Allow-Headers', 'Origin, Accept, Content-Type, Authorization, X-Requested-With, X-CSRF-Token');
//    if ('OPTIONS' == req.method){
//        return res.send(200);
//    }
    // Pass to next layer of middleware
    next();
});

// API Access link for creating client ID and secret:
// https://code.google.com/apis/console/
var GOOGLE_CLIENT_ID = "98399203155-bc4q9uhgqmru6uaqq49evlg1148mlgvr.apps.googleusercontent.com";
var GOOGLE_CLIENT_SECRET = "HYHSQN666rcu4XN8MPPluPWd";

passport.serializeUser(function (user, callback) {
    console.log('serializing user.');
    callback(null, user);
});

passport.deserializeUser(function (user, callback) {
    console.log('deserialize user.');
    callback(null, user);
});

var processRequest = function (request, accessToken, refreshToken, profile, callback) {
    //
    process.nextTick(function () {
        console.log('id : ' + profile.id);
        console.log('name :' + profile.displayName);
        console.log('email:' + profile.email);
        var objProfile = {
            id : profile.id,
            name: profile.displayName,
            email: profile.email
        };
        var photos = profile.photos;
        var i = 0;
        for ( var i in photos ) {
            console.log('photos: ' + i + ' * '  + photos[i].value);
            objProfile.photos =  photos[i].value;
            i++;
        };
        session.profile = objProfile;
        return callback(null, profile);
    });
};

passport.use(new GoogleStrategy({
    clientID: GOOGLE_CLIENT_ID,
    clientSecret: GOOGLE_CLIENT_SECRET,
    callbackURL: "http://luzicity.com.br:3000/auth/google/callback",
    passReqToCallback: true,
    realm: 'http://luzicity.com.br:3000'
    }, processRequest
));

// configure Express
app.set('views', __dirname + '/views');
app.set('view engine', 'ejs');
app.use(express.static(__dirname + '/public'));
app.use(cookieParser());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(session({
    secret: '123mudar',
    name: 'carlossantos',
    store: new RedisStore({
        host: 'luzicity.com.br',
        port: 3000
    }),
    proxy: true,
    resave: true,
    saveUninitialized: true
}));
app.use(passport.initialize());
app.use(passport.session());

app.get('/', function (req, res) {
    res.render('index', {user: req.user});
});

app.get('/account', ensureAuthenticated, function (req, res) {
    console.log(req.user.id + ' - ' + req.user.namedisplayName);
    res.render('account', {
        user: req.user
    });
});

app.get('/login', function (req, res) {
    res.render('login', {
        user: req.user
    });
});

app.get('/auth/google', 
    function (request, response, next) {
        passport.authenticate('google', {
            failureRedirect: '/login',
            session: false,
            scope: [
                'profile',
                'email'
            ]
        })(request, response, next);
    }
);

app.get('/auth/google/callback',
        passport.authenticate('google',
                {
                    successRedirect: 'http://luzicity.com.br:8100/#/app/auth/google/callback',
                    failureRedirect: '/login'
                })
        );

app.get('/profile', function (req, res) {
    console.log('app.get.profile');
    console.log(session.profile.id +' - '+ session.profile.name);
    res.send(JSON.stringify(session.profile));
});

app.get('/logout', function (req, res) {
    console.log('app.get.logout')
    req.logout();
    res.redirect('/login');
});

server.listen(3000);

// Simple route middleware to ensure user is authenticated.
//   Use this route middleware on any resource that needs to be protected.  If
//   the request is authenticated (typically via a persistent login session),
//   the request will proceed.  Otherwise, the user will be redirected to the
//   login page.
function ensureAuthenticated(req, res, next) {
    console.log('function.EnsureAuthenticated: ' + req.isAuthenticated());
    if (req.isAuthenticated()) {
        return next();
    }
    res.redirect('/login');
}