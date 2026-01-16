var db = require('./dbsetup');
var sqlUtils = require('./sqlutils');

// var initDatabase = async () => {
//   await db.init();
// }
//
// initDatabase().then( async () => {
//
//     for (i = 0; i < 10000; i++) {
//       let connection = db.getConnection();
//       sqlUtils.doSomething(await connection);
//    }
//
// });


 db.init().then(() => {

   for (i = 0; i < 10000; i++) {

      aPromise = db.getConnection()
      .then(function(conn) {
        sqlUtils.doSomething(conn);
      }).catch(function(err) {
        console.error(err);
      });
      
   }
 }).catch((err) => {
   console.log(err);
 })