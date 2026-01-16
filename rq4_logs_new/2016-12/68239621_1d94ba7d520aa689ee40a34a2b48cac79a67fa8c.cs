using dreamlet.DataAccessLayer.MongoDbContext;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dreamlet.DataAccessLayer.BootstarpDatabase
{
	public class DatabaseBootstrapper
	{
		[Import]
		public IMongoContext Context { get; set; }

		public string Text { get; set; }

		public DatabaseBootstrapper()
		{
			Text = System.IO.File.ReadAllText(@"Scrapes/dream-scrape1-take2-formatted.json");
		}

		public void InitialSeed()
		{
			//var document = BsonSerializer.Deserialize<BsonDocument>(text);
			//_context.Database.CreateCollection("dream_terms_test");
		}
	}
}