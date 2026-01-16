// --------------------------------------------------------------------------------------------------------------------
// <copyright file="BaseEntity.cs" company="N/A">
//   2017-2086
// </copyright>
// <summary>
//   The base MongoDb entity.
// </summary>
// --------------------------------------------------------------------------------------------------------------------
namespace BooksCore.Base
{
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization.Attributes;

    /// <summary>
    /// The base MongoDb entity.
    /// </summary>
    public class BaseEntity
    {
        /// <summary>
        /// Gets or sets the unique entity identifier.
        /// </summary>
        [BsonId]
        public ObjectId Id { get; set; }

        /// <summary>
        /// Gets or sets the entity name.
        /// </summary>
        [BsonElement("name")]
        public string Name { get; set; }
    }
}