using Newtonsoft.Json;
using System.Collections.Generic;

namespace Narochno.Slack.Entities
{
    public class MessageEventEdited
    {
        [JsonProperty("user")]
        public string User { get; set; }
        [JsonProperty("ts")]
        public string Timestamp { get; set; }
    }

    public class MessageEventReaction
    {
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("count")]
        public int Count { get; set; }
        [JsonProperty("users")]
        public IList<string> Users { get; set; } = new List<string>();
    }

    public class File
    {

    }

    public class Comment
    {

    }

    public class MessageEvent
    {
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("subtype")]
        public string SubType { get; set; }
        [JsonProperty("channel")]
        public string Channel { get; set; }
        [JsonProperty("user")]
        public string User { get; set; }
        [JsonProperty("username")]
        public string Username { get; set; }
        [JsonProperty("bot_id")]
        public string BotId { get; set; }
        [JsonProperty("text")]
        public string Text { get; set; }
        [JsonProperty("ts")]
        public string Timestamp { get; set; }
        [JsonProperty("deleted_ts")]
        public string DeletedTimestamp { get; set; }
        [JsonProperty("is_starred")]
        public bool IsStarred { get; set; }
        [JsonProperty("reactions")]
        public IList<MessageEventReaction> Reactions { get; set; } = new List<MessageEventReaction>();
        [JsonProperty("pinned_to")]
        public IList<string> PinnedTo { get; set; } = new List<string>();
        [JsonProperty("members")]
        public IList<string> Members { get; set; } = new List<string>();
        [JsonProperty("inviter")]
        public string Inviter { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("old_name")]
        public string OldName { get; set; }
        [JsonProperty("purpose")]
        public string Purpose { get; set; }
        [JsonProperty("topic")]
        public string Topic { get; set; }
        [JsonProperty("file")]
        public File File { get; set; }
        [JsonProperty("comment")]
        public Comment Comment { get; set; }
        [JsonProperty("upload")]
        public bool Upload { get; set; }
        [JsonProperty("hidden")]
        public bool Hidden { get; set; }
        [JsonProperty("message")]
        public MessageEvent Message { get; set; }
        [JsonProperty("item_type")]
        public string ItemType { get; set; }
        [JsonProperty("item")]
        public MessageEvent Item { get; set; }
    }
}