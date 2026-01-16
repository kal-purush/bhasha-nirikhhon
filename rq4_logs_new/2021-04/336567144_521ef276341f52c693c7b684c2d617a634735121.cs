using System.Reflection;
using System.Text.Json.Serialization;
using MobileDevelopment.Extensions;
using MobileDevelopment.Views;
using Xamarin.Forms;

namespace MobileDevelopment.Models
{
    public class BookDetail
    {
        [JsonPropertyName("title")] 
        public string Title { get; set; }

        [JsonPropertyName("subtitle")] 
        public string SubTitle { get; set; }
        [JsonPropertyName("price")] 
        public string Price { get; set; }
        
        [JsonPropertyName("authors")] 
        public string Authors { get; set; }
        
        [JsonPropertyName("publisher")] 
        public string Publisher { get; set; }

        [JsonPropertyName("pages")] 
        public string Pages { get; set; }

        [JsonPropertyName("year")] 
        public string Year { get; set; }

        [JsonPropertyName("rating")] 
        public string Rating { get; set; }

        [JsonPropertyName("desc")] 
        public string Description { get; set; }

        [JsonPropertyName("image")]
        public string Image { get; set; }

        public ImageSource ImageSource => ImageSource.FromResource(string.Concat(Constants.RESOURCES, Image), typeof(BookDetailPage).GetTypeInfo().Assembly);
    }
}