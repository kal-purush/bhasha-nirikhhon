 

var async = require('async');

 


exports.persistency_set = function(req, res) {
	
	console.log( "persistency set request"  );
	
	var key = req.params.key;
	var value = req.params.value;
   
	var clientRedis = req.app.get('clientRedis');	
	
	clientRedis.set( key, value, function(err, reply) {
	  console.log( "persistency set respaonse" + reply );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(reply);
	  }
	});
	 
	  
};
exports.persistency_hset = function(req, res) {
	
	
	
	var n = req.params.name;
	var v = req.params.value;
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	console.log( "persistency hset request athleteid:" + athleteid + " appliname:" + appliname  );
	console.log( "persistency hset name:" + n + " value:" + v  );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency hset request key:" + key  );
	
	var data = {};
	data[n] = v;
	
	clientRedis.hmset( key, data, function(err, reply) {
	  console.log( "persistency hset response" + reply );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(reply);
	  }
	});
	 
	  
};
exports.persistency_hget = function(req, res) {
	
	 
 
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	var name = req.params.name;
	
	console.log( "persistency hget request athleteid:" + athleteid + " appliname:" + appliname  );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency hget request key:" + key  );
	
	clientRedis.hmget( key, name, function(err, reply) {
	  console.log( "persistency hget response err:" + err + " reply lg:" + reply.length + " reply:" + JSON.stringify( reply ) );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(reply);
	  }
	});
	 
	  
};

exports.persistency_hgetall = function(req, res) {
	
	 
 
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	 
	
	console.log( "persistency hgetall request athleteid:" + athleteid + " appliname:" + appliname  );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency hgetall request key:" + key  );
	
	clientRedis.hgetall( key, function(err, reply) {
	  console.log( "persistency hgetall response err:" + err + " reply:" + JSON.stringify( reply ) );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(reply);
	  }
	});
	 
	  
};
exports.persistency_lpop = function(req, res) {
	
	 
 
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	 
	
	console.log( "persistency lpop request athleteid:" + athleteid + " appliname:" + appliname  );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency lpop request key:" + key  );
	
	clientRedis.lpop( key, function(err, reply) {
	  console.log( "persistency lpop response err:" + err + " reply:" + JSON.stringify( reply ) );
	  if ( err ){
			res.status(500).send(err);
	  }else{
		  
		  if ( !reply ){
			res.json({error:"end of list"});   
		  }else{
			res.json({data:reply});
		  }
	  }
	});
	 
	  
};
exports.persistency_lpush_post = function(req, res) {
	
	 
 
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	
	var value = req.body;
	if ( !value ) {
		res.status(500).send("no body defined");
	}
	var valueStr = JSON.stringify(value);
	
	 
	
	console.log( "persistency lpush request athleteid:" + athleteid + " appliname:" + appliname + " valueStr:" + valueStr );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency lpush request key:" + key  );
	
	clientRedis.lpush( key, valueStr, function(err, reply) {
	  console.log( "persistency lpush response err:" + err + " reply:" + JSON.stringify( reply ) );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(200)
	  }
	});
	 
	  
};
exports.persistency_lpush_get = function(req, res) {
	
	 
 
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	var valueStr = req.query.value;
	//var valueStr = req.body.value;
	//if ( !valueStr ) return;
	//var value = JSON.parse(valueStr);
	
	 
	
	console.log( "persistency lpush request athleteid:" + athleteid + " appliname:" + appliname + " valueStr:" + valueStr );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency lpush request key:" + key  );
	
	clientRedis.lpush( key, valueStr, function(err, reply) {
	  console.log( "persistency lpush response err:" + err + " reply:" + JSON.stringify( reply ) );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(200)
	  }
	});
	 
	  
};

exports.persistency_del = function(req, res) {
	
	 
 
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	 
	
	console.log( "persistency del request athleteid:" + athleteid + " appliname:" + appliname + " valueStr:" + valueStr );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency del request key:" + key  );
	
	clientRedis.del( key, function(err, reply) {
	  console.log( "persistency del response err:" + err + " reply:" + JSON.stringify( reply ) );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(200)
	  }
	});
	 
	  
};
exports.persistency_keys = function(req, res) {
	
	 
 
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	 
	
	console.log( "persistency keys request athleteid:" + athleteid + " appliname:" + appliname + " valueStr:" + valueStr );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency keys request key:" + key  );
	
	clientRedis.keys( key, function(err, reply) {
	  console.log( "persistency keys response err:" + err + " reply:" + JSON.stringify( reply ) );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(reply)
	  }
	});
	 
	  
};
exports.persistency_llen = function(req, res) {
	
	 
 
	var appliname = req.params.appliname;
	var athleteid = req.params.athleteid;
	
	 
	
	console.log( "persistency llen request athleteid:" + athleteid + " appliname:" + appliname + " valueStr:" + valueStr );
   
	var clientRedis = req.app.get('clientRedis');	
	
	var key = athleteid + ":" + appliname;
	console.log( "persistency llen request key:" + key  );
	
	clientRedis.llen( key, function(err, reply) {
	  console.log( "persistency llen response err:" + err + " reply:" + JSON.stringify( reply ) );
	  if ( err ){
			res.status(500).send(err);
	  }else{
			res.json(reply)
	  }
	});
	 
	  
};
exports.persistency_index = function(req, res) {
	
	console.log( "persistency index request"  );
	
	
	  
};