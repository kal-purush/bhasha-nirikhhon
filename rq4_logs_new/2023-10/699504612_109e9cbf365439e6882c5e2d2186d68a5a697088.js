const mongoose = require('mongoose');

//Import the Citzen and User models.
const Artwork = require('./models/ArtworkModel.js');
const User = require('./models/UserModel.js');
const Post = require('./models/PostModel.js');

// get data from gallery json file
const gallery = require('./data/gallery.json');

// Array of registered users.
const users = require('./data/users.json');

//Create an async function to load the data.
//Other mongoose calls that return promises(connect, dropdatabase, create) 
//inside the async function can use an await.
const loadData = async () => {

    //Connect to the mongo database.
    await mongoose.connect('mongodb://localhost:27017/opengallery');

    //Remove database and start anew.
    await mongoose.connection.dropDatabase();

    //Map each artwork object into a new artwork model.
    let artworks = gallery.map(artwork => new Artwork(artwork));

    //Map each registered user object into the a new User model.
    let userList = users.map(aUser => new User(aUser));

    //Create a new posted artwork for each artwork.
    for (let i = 0; i < artworks.length; i++) {
        // loop through users list
        // for each user, create a new posted artwork
        // save the posted artwork to the database
        for (let j = 0; j < userList.length; j++) {
            // if artowrk artist is equal to username
            if (artworks[i].artist === userList[j].username) {
                let post = new Post({
                    artwork: artworks[i]._id,
                    user: userList[j]._id,
                });
                await post.save();
            }
        }
    }




    //Creates a new documents of a artworks and user and saves them to the database.
    await Artwork.create(artworks);
    await User.create(userList);
}

//Call to load the data.
//Once the loadData Promise returns it will close the database
//connection.  Any errors from connect, dropDatabase or create
//will be caught in the catch statement.
loadData()
    .then((result) => {
        console.log("Closing database connection.");
        mongoose.connection.close();
    })
    .catch(err => console.log(err));