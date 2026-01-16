var express = require( 'express' );
var _ = require( 'underscore' );
var sockjs = require( 'sockjs' );
var http = require( 'http' );

var app = express();

app.use(express.static( 'client' ));

var chat = sockjs.createServer();

var connections = new Array();

chat.on( 'connection', function( conn ){
	
	connections.push( conn );

	conn.on( 'data', function( message ){
		
		var _cons = _.reject( connections, function( _c ){
			return _c.id == conn.id;	
		});

		_.each( _cons, function( _c ){
			_c.write( message );	
		});

	});

});

var server = http.createServer( app ).listen( 3000 );

chat.installHandlers( server, { prefix: '/chat' });
