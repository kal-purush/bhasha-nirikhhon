using System.ComponentModel.DataAnnotations;

namespace ExploreGetRssFeed.Models
{
    public class FeedEntryModel
    {
        [Required(ErrorMessage = "Make up a short title for this RSS Feed.")]
        [StringLength(maximumLength: 30, MinimumLength = 3)]
        public string Title { get; set; }

        [Required]
        [Url(ErrorMessage = "The url does not match an expected format.")]
        public string WebAddress { get; set; }

        public string BaseUrl = "/rssfeeds/";

        public string? PathUrl
        {
            get
            {
                return string.Concat(BaseUrl, Title.Replace(" ", ""));
            }
        }

        public string ToRow()
        {
            return $"{Title}: {WebAddress}";
        }
    }
}