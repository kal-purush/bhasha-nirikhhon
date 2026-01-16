import { MongoClient } from "mongodb";

const connectDB = async (operation,res) => {
    /*
    0. create a higher order function that connects to database
    1. store them in a try catch block for error
    2. connect to server of database -> grab the database we're using
    3. call the callback function(operation) to start executing - this operation is modifying the database and modifying the database is an async
    4. close the connection to database
    */
    try {
        const client = await MongoClient.connect("mongodb://localhost:27017", { useNewUrlParser: true });
        const db = client.db("listItem");

        await operation(db);

        client.close();
    } catch (err) {
        // pass the response from api 
        res.status(500).json({ message: "error connecting to database" });
    } 
}

export default connectDB;