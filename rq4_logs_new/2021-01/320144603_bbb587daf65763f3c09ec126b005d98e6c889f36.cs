using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Platform.Lobby.Api.Enums;

namespace Platform.Lobby.Api.Data.Models
{
	public class Lobby
	{
		[BsonId]
		[BsonRepresentation(BsonType.ObjectId)]
		public string Id { get; set; } = null!;
		public string? Code { get; set; }
		public List<Player> Players { get; set; } = new List<Player>();
		public Visibility? Visibility { get; set; }
		public Status? Status { get; set; }
		public List<ChatMessage> LobbyMessages { get; set; } = new List<ChatMessage>();
	}
}