const mongoose = require("mongoose");
const URI = "mongodb://localhost/marcianos";

module.exports = () => {
    mongoose.connect(URI, { useNewUrlParser: true, useFindAndModify: false })
    .then(db => console.log(`DB is connected ${URI}`))
    .catch(err => console.log(err));

    process.on("SIGINT", () => {
        mongoose.connection.close( () => {
            console.log("Mongo is disconnected");
            process.exit(0);
        });
    });
}