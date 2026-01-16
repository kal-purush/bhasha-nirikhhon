using Aksio.MongoDB;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Samples;

MongoDBDefaults.Initialize();

BsonClassMap.RegisterClassMap<Blah>(cm =>
{
    cm.AutoMap();
    cm.MapField(_ => _.Foo).SetSerializer(BsonSerializer.LookupSerializer<object>());
    // cm.SetDiscriminator("Blah");
    // cm.MapField("_bar").SetElementName("bar");
});

// BsonClassMap.RegisterClassMap<Bar>(cm =>
// {
//     cm.AutoMap();
//     cm.SetDiscriminator(typeof(Bar).AssemblyQualifiedName);
// });

var client = new MongoClient("mongodb://localhost:27017");
var db = client.GetDatabase("test");
db.DropCollection("blahs");
var collection = db.GetCollection<Blah>();

var b = new Blah("Hello", 42, new Bar("aasd"));
var doc = b.ToBsonDocument();

// collection.InsertOne(new Blah("Hello", 42, "Something"));
collection.InsertOne(new Blah("Hello", 42, new Bar("aasd")));
collection.InsertOne(new Blah("Hello", 42, new Foo("Cat", "Horse", "Bar")));
collection.InsertOne(new Blah("Hello", 42, new Foo("Cat", "Horse", new Bar("Something"))));


// var items = collection.Find(_ => true).ToList();
collection.Find(_ => true).ToList().ForEach(Console.WriteLine);