console.log('server test');

// var settings = require('server/exercises/users');  uso il file settings.json che contiene url di collegamento al db
var mongoose = require('mongoose');
mongoose.Promise = require('bluebird');  // importiamo le promise di angular in mongodb grazie a bluebird
mongoose.connect('mongodb://artoo:niki@ds019123.mlab.com:19123/artoo_corso', function (err){
   console.info(err); 
});


var Schema = mongoose.Schema;   // setto lo schema del db
var userSchema = new Schema({
    name: String,
    birthday: Date,
    surname: String,
    id: String,
    updatedAt: Date,   // campo usato per salvare la data di creazione dell user
});

userSchema.methods.greet = function (name) {
    console.log('Ciao ' + name + ' da ' + this.name);  
};

userSchema.methods.sayAge = function () {
  var today = new Date();
  var age= today.getFullYear() - this.birthday.getFullYear();
  console.log('Io ho ' + age + ' anni ');
};

//middlewares

userSchema.pre('save', function (next){
   this.updatedAt = new Date (); 
   console.log('Utente ' + this.name + ' salvato il ' + this.updatedAt);
   var err;
   if (this.name.length > 10) err = new Error ('Nome troppo corto');   // condizione per entrare nel salvataggio
   next(err);  // inserendo una condizione posso stoppare il salvataggio
});

var User = mongoose.model('User', userSchema);

var bob = new User({
   name: 'Bob Doe', 
   birthday: new Date(1992, 10, 12),
   id: '01',
});

bob.save();

bob.sayAge();
// console.log(bob);      bob.id restituisce id dell user
bob.greet('anna');

/*bob.save().then(() =>{
    console.log('utente salvato');
}).catch((err) =>{
   console.error('errore salvataggio' +err); 
});*/

// var query = User.find(); permette di storare il database in una query e riusarla

User.findOne({name: 'Bob Doe'}).exec().then((user) => {  // find viene usato per la creazione di query
                                                         // findOne si ferma alla prima ricerca del campo {age: 43} e rispetto alla find restituisce un oggetto
                                                        // findById ('01') permette di cercare un campo del db tramite ID
   console.info(user);  // lettura da un database
   
   
   user.name = 'Bob1';
   user.surname = 'Doe1';
   return user.save();    // quando  ritorno una promessa( save ritorna una promessa) posso concatenarla con un then

    
}).then(() => {
   console.info('salvataggio riuscito'); 
}).catch((err) => {
   console.error(err); 
});



var express = require('express');
var app = express();

app.listen(8080, function (){
    console.info('database online');
});