var MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost:27017/weather', function(err, db){
  if (err) throw err
  var cursor = db.collection('data').find();
  var order = cursor.sort({State: 1 , Temperature: -1});
  var operator = { $set: {"month_high": true}};
  var lastDoc = 0;
  order.each(function(err, doc){
    if (err) throw err;
    if (doc == null){
      return db.close();
    }
    if (doc.State == !lastDoc){
      doc.update({}, operator, function(err, updated){
        if (err) throw err
        if (updated) {
          console.log( "Success " + update);
          lastDoc = doc.State;
        };
      })
    };
    // if
    // console.log(doc.State + ' has weather');
  });
});