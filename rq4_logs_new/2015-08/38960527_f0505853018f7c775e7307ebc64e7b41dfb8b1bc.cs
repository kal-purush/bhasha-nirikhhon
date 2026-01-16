namespace Encore.DataStore.Migrations
{
    using MongoDB.Bson;
    using MongoDB.Driver;
    using MongoMigrations;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    public class ChangeFieldProjectIdToString : CollectionMigration
    {
        public ChangeFieldProjectIdToString() : base("1.0.0", "Field")
        {
            Description = "Change Field ProjectIds from int to string";
        }

        public override void UpdateDocument(MongoCollection<BsonDocument> collection, BsonDocument document)
        {
            var projectIds = document.GetValue("ProjectIds").AsBsonArray.Select(p => p.AsInt32).ToList();
            document.Remove("ProjectIds");
            document.Add("ProjectIds", new BsonArray(projectIds.Select(x => x.ToString())));
            collection.Save(document);
        }
    }
}