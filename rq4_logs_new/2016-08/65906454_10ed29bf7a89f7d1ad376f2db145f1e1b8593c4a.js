var restify = require('restify');
var request = require('request');
var mongojs = require('mongojs');

var db = mongojs('mongodb://RealFighter64:ultraminer01@ds042459.mlab.com:42459/pizzainmytummy', ['groups']);

var server = restify.createServer();
server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser());
server.use(function(req, res, next) {
    res.header('Access-Control-Allow-Origin', "*");
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
});

server.get('/api/getGroup/:id', function(req, res, next) {
    db.groups.findOne({
        _id: mongojs.ObjectId(req.params.id)
    }, function(err, doc) {
        res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8'
        });
        res.end(JSON.stringify(doc));
    });
});

server.post('/api/newGroup', function(req, res, next) {
    var group = {};
    group.name = req.params.name;
    group.list = [];
    group.password = req.params.password;

    db.groups.save(group, function(err, doc) {
        res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8'
        });
        res.end(JSON.stringify(doc));
    });
    return next();
});

server.post('/api/addItem/:id', function(req, res, next) {
    var item = req.params;
    item.toppings = req.params.toppings.split(',');
    var id = req.params.id;
    console.log(id);
    db.groups.update({
            _id: mongojs.ObjectId(id)
        }, {
            $push: {
                list: item
            }
        }, {
            multi: false
        },
        function(err, data) {
            res.writeHead(200, {
                'Content-Type': 'application/json; charset=utf-8'
            });
            res.end(JSON.stringify(data));
        }
    );
});

server.del('/api/removeGroup/:id', function(req, res, next) {
    db.groups.remove({
        _id: mongojs.ObjectId(req.params.id)
    }, true, function(err, data) {
        res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8'
        });
        res.end(JSON.stringify());
    })
});

server.listen(process.env.PORT || 9804, function() {
    console.log("Server started @ ", process.env.PORT || 9804);
});