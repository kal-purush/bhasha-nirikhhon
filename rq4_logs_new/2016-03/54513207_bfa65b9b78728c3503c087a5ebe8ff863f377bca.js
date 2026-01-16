//-------------- requires ----------------
var restify = require('restify');
var adal = require('adal-node').AuthenticationContext;
var https = require('https');

//--------------- setup ------------------
var server = restify.createServer();
server.use(restify.bodyParser({ mapParams: false }));

//-------------- routes ------------------ 
server.get('/All', function (req, res, next) { 
    console.log("> Received GET: All");
    
    authenticate(function (err, token){
        if (err) {
            console.log(err);
            res.send(err);
            return;
        }

        getAllUsers(token, function(err, userlist){
            if (err) {
                res.send(e);
                return;
            }

            res.send(userlist);
        });
    });
    
    next();
});

server.post('/', function (req, res, next) { 
    console.log("> Received POST /");
    console.log("> Body: " + req.body);
    
    authenticate(function (err, token){
        if (err) {
            console.log(err);
            res.send(err);
            return;
        }

        createUser(token, req.body, function(err, response){
            if (err) {
                res.send(e);
                return;
            }

            res.send(response);
        });
    });
    
    next();
});

server.post('/:userid', function (req, res, next) { 
    console.log("> Received POST: " + req.params.userid);
    console.log("> Body: " + req.body);
    
    authenticate(function (err, token){
        if (err) {
            console.log(err);
            res.send(err);
            return;
        }

        updateUser(token, req.params.userid, req.body, function(err, response){
            if (err) {
                res.send(e);
                return;
            }

            res.send(response);
        });
    });
    
    next();
});

//-------------- functions -------------------
function authenticate (next) {
    var authorityHostUrl = 'https://login.windows.net';
    var tenant = 'argoadb2ctest.onmicrosoft.com';
    var authorityUrl = authorityHostUrl + '/' + tenant;
    var clientId = 'ee130d60-a521-4487-b003-a58abc9c9ab4';
    var clientSecret = 'OfFnYiTvVTn3mM5th4TpN7/6qgQf7P43mOabn/Y8lgo='
    var resource = '00000002-0000-0000-c000-000000000000';
    
    var context = new adal(authorityUrl);
    
    console.log("> Authority: " + authorityUrl);

    context.acquireTokenWithClientCredentials(resource, clientId, clientSecret, function(err, tokenResponse) {
        if (err) {
            console.log("> Authentication Failed.");
            next(err, null);
            return;
        }

        console.log("> Authentication Successful.");
        next(null, tokenResponse.accessToken);
    }); 
}

function getHttpOptions(token, method, path) {
    var options = {
        host: 'graph.windows.net',
        port: 443,
        path: '/argoadb2ctest.onmicrosoft.com' + path + '?api-version=1.6',
        method: method,
        headers: {
            'Authorization': 'Bearer ' + token,
            'Content-Type': 'application/json'
        }
    };
    
    return options;
}

function handleHttpResult(result, next) {
    var resultdata = '';
    
    result.on("data", function (chunk){
        resultdata += chunk;
    });
    
    result.on("end", function (chunk){
        responseobj = {};
        if (resultdata) {
            var responseobj = JSON.parse(resultdata);                
        }
        next(responseobj);
    });
}

function getAllUsers(token, next) {
    var httpoptions = getHttpOptions(token, 'GET', "/users");

    https.get(httpoptions, function(result) {
        handleHttpResult(result, function(obj) {
            next(null, obj);
        });
    }).on('error', function(e) {
        next(e, null);
    });
}

function updateUser(token, userid, userjson, next) {
    var httpoptions = getHttpOptions(token, 'PATCH', '/users/' + userid);
    
    var req = https.request(httpoptions, function (result) {
        handleHttpResult(result, function(obj) {
            next(null, obj);
        });
    }).on('error', function(e) {
        next(e, null);
    });
    
    req.write(JSON.stringify(userjson));
    req.end();
}

function createUser(token, userjson, next) {
        var httpoptions = getHttpOptions(token, 'POST', '/users');
    
    var req = https.request(httpoptions, function (result) {
        handleHttpResult(result, function(obj) {
            next(null, obj);
        });
    }).on('error', function(e) {
        next(e, null);
    });
    
    req.write(JSON.stringify(userjson));
    req.end();
}

//----------- start server --------------- 
var port = process.env.PORT || 8080;
server.listen(port, function() {
  console.log('%s listening at %s', server.name, server.url);
});