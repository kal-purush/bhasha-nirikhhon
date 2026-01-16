import { MongoClient } from "mongodb";
const uri ="mongodb+srv://aniket:Sigdvpk5I6uIkMZE@cluster0.rhm8d.mongodb.net/shop?retryWrites=true&w=majority";


let _db ;

export const MongoConnet = callback =>{

// const client = new MongoClient(uri, {
//   useNewUrlParser: true,
//   useUnifiedTopology: true,
//   serverApi: ServerApiVersion.v1,
// });

// client.connect((err) => {
//   const collection = client.db("test").collection("devices");
//   // perform actions on the collection object
//   client.close();
// });

MongoClient.connect(uri)
.then((client)=>{
    console.log("connected");
    _db = client.db();
    // callback(client);
})
.catch((err)=>{
    console.log(err);
});

}

export const getDb = ()=>{

    if(_db){
        return _db;
    }

    throw "No database found";
}