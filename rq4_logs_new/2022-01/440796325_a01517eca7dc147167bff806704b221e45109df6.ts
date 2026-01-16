/**
 * dbConnector for mongodb with Mongoose
**/
import mongoose from "mongoose";

const dbuser: string | undefined = process.env.DBUSER;
let dbpwd: string | undefined = process.env.DBPWD;

let authString: string = '';
if (dbuser && dbpwd) {
    dbpwd = encodeURIComponent(dbpwd);
    authString = `${dbuser}:${dbpwd}@`
}


mongoose.connect(`mongodb://${authString}localhost:27017/${process.env.DBNAME}?authSource=admin`);

const dbConnector = mongoose.connection;
dbConnector.on('error', () => {
    console.error("Error while connecting to DB");
});

console.log('connected to DB');

export { dbConnector };