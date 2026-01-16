let osmosis = require("osmosis");
let fs = require("fs");
const MongoClient = require("mongodb").MongoClient;

const url = "mongodb://localhost:27017/";
const mongoClient = new MongoClient(url, { useNewUrlParser: true });

osmosis
  .get("https://auction.violity.com/") // ссылка которую парсим
  .find(".tabs_txt.active")
  .set({ related: [".cont .title, .price"] })
  .data(function(listing) {
    let data = {},
      data1 = {};
    for (let i = 0; i < listing.related.length; i++) {
      data1[i] = listing.related[i];
    }
    data.lots = data1;
    console.log(data);
    fs.appendFileSync("index.html", JSON.stringify(data), function(err) {
      if (err) throw err;
      console.log("Saved!");
    });
    mongoClient.connect(function(err, client) {
      const db = client.db("labaParse"); 
      const collection = db.collection("test"); 
      collection.insertOne(data, function(err, result) {
        if (err) {
          return console.log(err);
        }
        client.close();
      });
    });
  });