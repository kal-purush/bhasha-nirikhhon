using Education.Data.Abstracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Education.Data.Concreat
{
    public class QuestionsRepository : IQuestionsRepository
    {


        /// <summary>
        /// Query the questions using the filter.
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        public IEnumerable<Question> GetQuestions(BsonDocument filter)
        {
            var collection = Shared.GetSharedSingleton._database.GetCollection<BsonDocument>("questions");

            //var filter = null; // Builders<BsonDocument>.Filter();

            var result = collection.Find(null).ToList();

            var list = new List<Question>();

            foreach (var item in result)
            {
                list.Add(Helpers.Helper.CreateQuestion(item));
            }

            return list.ToArray();
        }




        /// <summary>
        /// Add a question to database.
        /// </summary>
        /// <param name="question"></param>
        public void AddQuestion(Question question)
        {
            throw new NotImplementedException();
        }


        /// <summary>
        /// Update an existing question.
        /// </summary>
        /// <param name="question"></param>
        public void UpdateQuestion(Question question)
        {
            throw new NotImplementedException();
        }
    }






    class Program
    {
        protected static IMongoClient _client;
        protected static IMongoDatabase _database;

        static void Main(string[] args)
        {
            _client = new MongoClient();
            _database = _client.GetDatabase("test");

            //tryInsert();
            //tryQuery();
            //tryUpdate();
            tryAgg();
        }


        async static void tryInsert()
        {

            var document = new BsonDocument
            {
                { "address", new BsonDocument
                    {
                        { "street", "2 Avenue" },
                        { "zipcode", "10075" },
                        { "building", "1480" },
                        { "coord", new BsonArray { 73.9557413, 40.7720266 } }
                    }
                },
                { "borough", "Manhattan" },
                { "cuisine", "Italian" },
                { "grades", new BsonArray
                    {
                        new BsonDocument
                        {
                            { "date", new DateTime(2014, 10, 1, 0, 0, 0, DateTimeKind.Utc) },
                            { "grade", "A" },
                            { "score", 11 }
                        },
                        new BsonDocument
                        {
                            { "date", new DateTime(2014, 1, 6, 0, 0, 0, DateTimeKind.Utc) },
                            { "grade", "B" },
                            { "score", 17 }

                        }
                    }
                },
                { "name", "Vella" },
                { "restaurant_id", "41704620" }
            };

            var collection = _database.GetCollection<BsonDocument>("restaurants");
            await collection.InsertOneAsync(document);

        }


        static void tryQuery()
        {
            var collection = _database.GetCollection<BsonDocument>("restaurants");

            //var filter = new BsonDocument();
            //var filter = Builders<BsonDocument>.Filter.Eq("borough", "Manhattan");
            //var filter = Builders<BsonDocument>.Filter.Eq("address.zipcode", "10075");
            //var filter = Builders<BsonDocument>.Filter.Eq("grades.grade", "B");
            //var filter = Builders<BsonDocument>.Filter.Lt("grades.score", 10);         // less than
            var filter = Builders<BsonDocument>.Filter.Eq("cuisine", "Italian") | Builders<BsonDocument>.Filter.Eq("address.zipcode", "10075");         // combined

            var count = 0;

            var result = collection.Find(filter).ToList();

            Console.WriteLine("Count = " + result.Count.ToString());

            var i = 0;

            foreach (var doc in result)
            {
                Console.WriteLine(doc.GetValue("_id"));

                i++;

                if (i > 10) break;
            }

            //using (var cursor = await collection.FindAsync(filter))
            //{
            //    while (await cursor.MoveNextAsync())
            //    {
            //        var batch = cursor.Current;
            //        foreach (var documant in batch)
            //        {
            //            // process document
            //            count++;
            //        }
            //    }
            //}

            Console.ReadLine();
        }


        static void tryQuery_2()
        {
            var collection = _database.GetCollection<BsonDocument>("restaurants");

            //var filter = new BsonDocument();
            //var filter = Builders<BsonDocument>.Filter.Eq("borough", "Manhattan");
            //var filter = Builders<BsonDocument>.Filter.Eq("address.zipcode", "10075");
            //var filter = Builders<BsonDocument>.Filter.Eq("grades.grade", "B");
            //var filter = Builders<BsonDocument>.Filter.Lt("grades.score", 10);         // less than
            var filter = Builders<BsonDocument>.Filter.Eq("cuisine", "Italian") | Builders<BsonDocument>.Filter.Eq("address.zipcode", "10075");         // combined

            var sort = Builders<BsonDocument>.Sort.Ascending("borough").Ascending("address.zipcode");


            var result = collection.Find(filter).Sort(sort).ToList();

            Console.WriteLine("Count = " + result.Count.ToString());

            Console.ReadLine();


        }
        // query by a field in an embadded document.


        static void tryUpdate()
        {
            var collection = _database.GetCollection<BsonDocument>("restaurants");

            var filter = Builders<BsonDocument>.Filter.Eq("name", "Juni");

            var update = Builders<BsonDocument>.Update
                .Set("cuisine", "American (New)")
                .CurrentDate("lastModified");

            var result = collection.UpdateOne(filter, update);

            Console.WriteLine("Modified count = " + result.MatchedCount);

            Console.ReadLine();

        }


        static void tryAgg()
        {
            var collection = _database.GetCollection<BsonDocument>("restaurants");
            var aggregate = collection.Aggregate().Group(new BsonDocument { { "_id", "$borough" }, { "count", new BsonDocument("$sum", 1) } });



        }


    }
}