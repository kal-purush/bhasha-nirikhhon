using Newtonsoft.Json;
using System.Collections.Generic;

namespace Narochno.Slack.Entities
{
    public class File
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("created")]
        public long Created { get; set; }
        [JsonProperty("timestamp")]
        public long Timestamp { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("title")]
        public string Title { get; set; }
        [JsonProperty("mimetype")]
        public string MimeType { get; set; }
        [JsonProperty("filetype")]
        public string FileType { get; set; }
        [JsonProperty("pretty_type")]
        public string PrettyType { get; set; }
        [JsonProperty("user")]
        public string User { get; set; }
        [JsonProperty("mode")]
        public string Mode { get; set; }
        [JsonProperty("editable")]
        public bool Editable { get; set; }
        [JsonProperty("is_external")]
        public bool IsExternal { get; set; }
        [JsonProperty("external_type")]
        public string ExternalType { get; set; }
        [JsonProperty("username")]
        public string Username { get; set; }
        [JsonProperty("size")]
        public long Size { get; set; }
        [JsonProperty("url_private")]
        public string UrlPrivate { get; set; }
        [JsonProperty("url_private_download")]
        public string UrlPrivateDownload { get; set; }
        [JsonProperty("thumb_64")]
        public string Thumb64 { get; set; }
        [JsonProperty("thumb_360")]
        public string Thumb360 { get; set; }
        [JsonProperty("thumb_360_gif")]
        public string Thumb360Gif { get; set; }
        [JsonProperty("thumb_360_w")]
        public int Thumb360Width { get; set; }
        [JsonProperty("thumb_360_h")]
        public int Thumb360Height { get; set; }
        [JsonProperty("thumb_480")]
        public string Thumb480 { get; set; }
        [JsonProperty("thumb_480_w")]
        public int Thumb480Width { get; set; }
        [JsonProperty("thumb_480_h")]
        public int Thumb480Height { get; set; }
        [JsonProperty("thumb_160")]
        public string Thumb160 { get; set; }
        [JsonProperty("permalink")]
        public string Permalink { get; set; }
        [JsonProperty("permalink_public")]
        public string PermalinkPublic { get; set; }
        [JsonProperty("edit_link")]
        public string EditLink { get; set; }
        [JsonProperty("preview")]
        public string Preview { get; set; }
        [JsonProperty("preview_highlight")]
        public string PreviewHighlight { get; set; }
        [JsonProperty("lines")]
        public int Lines { get; set; }
        [JsonProperty("lines_more")]
        public int LinesMore { get; set; }
        [JsonProperty("is_public")]
        public bool IsPublic { get; set; }
        [JsonProperty("public_url_shared")]
        public bool PublicUrlShared { get; set; }
        [JsonProperty("display_as_bot")]
        public bool DisplayAsBot { get; set; }
        [JsonProperty("channels")]
        public IList<string> Channels { get; set; } = new List<string>();
        [JsonProperty("groups")]
        public IList<string> Groups { get; set; } = new List<string>();
        [JsonProperty("ims")]
        public IList<string> IMs { get; set; } = new List<string>();
        // initial_comment ?
        [JsonProperty("num_stars")]
        public int NumStars { get; set; }
        [JsonProperty("is_starred")]
        public bool IsStarred { get; set; }
        [JsonProperty("pinned_to")]
        public IList<string> PinnedTo { get; set; } = new List<string>();
        [JsonProperty("reactions")]
        public IList<MessageEventReaction> Reactions { get; set; } = new List<MessageEventReaction>();
        [JsonProperty("comments_count")]
        public int CommentsCount { get; set; }
    }
}