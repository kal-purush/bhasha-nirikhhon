module.exports = {

// post

// get


// get


// put


// delete

create: (req, res) => {
   const dbInstance = req.app.get('db');

   dbInstance.create_product()
   .then( () => res.sendStatus(200))
   .catch( err => {
      res.sendStatus(500).send({errorMessage: "Website currently under maintenance"});
      console.log(err);
   });
},


getOne: (req, res) => {
   const dbInstance = req.app.get('db');

   dbInstance.read_product()
   .then( () => res.sendStatus(200))
   .catch( err => {
      res.sendStatus(500).send({errorMessage: "Website currently under maintenance"});
      console.log(err);
   });
},




getAll: (req, res) => {
   const dbInstance = req.app.get('db');

   dbInstance.read_products()
   .then( () => res.sendStatus(200))
   .catch( err => {
      console.log(err);
      res.sendStatus(500).send({errorMessage: "Website currently under maintenance"});
   });
},



update: (req, res) => {
   const dbInstance = req.app.get('db');

   dbInstance.update_product()
   .then( () => res.sendStatus(200))
   .catch( err => {
      res.sendStatus(500).send({errorMessage: "Website currently under maintenance"});
      console.log(err);
   });
},



delete: (req, res) => {
   const dbInstance = req.app.get('db');

   dbInstance.delete_product()
   .then( () => res.sendStatus(200))
   .catch( err => {
      res.sendStatus(500).send({errorMessage: "Website currently under maintenance"});
      console.log(err);
   });
},

}