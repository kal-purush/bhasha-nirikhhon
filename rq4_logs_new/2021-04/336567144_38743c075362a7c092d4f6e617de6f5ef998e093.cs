using System.Text.Json.Serialization;

namespace MobileDevelopment.Models
{
    public class Book
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }
        
        [JsonPropertyName("title")]
        public string Title { get; set; }
        
        [JsonPropertyName("subtitle")]
        public string SubTitle { get; set; }
        
        [JsonPropertyName("isbn13")]
        public string Isbn13 { get; set; }
        
        [JsonPropertyName("price")]
        public string Price { get; set; }
        
        [JsonPropertyName("image")]
        public string Image { get; set; }
    }
}