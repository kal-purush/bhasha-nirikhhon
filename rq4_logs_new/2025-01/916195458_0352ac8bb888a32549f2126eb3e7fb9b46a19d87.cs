using Microsoft.AspNetCore.Components;
using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;

namespace ExploreGetRssFeed.Models
{
    public class FeedItem
    {
        public int? Id { get; set; }
        [Display(Name = "Title")]
        public string Title { get; set; } = "Untitled";
        [Display(Name = "Link")]
        public string Link { get; set; } = string.Empty;
        [Display(Name = "Creator")]
        public string Creator { get; set; } = "Unlisted";
        [Display(Name = "Publish Date")]
        public DateTime PubDate { get; set; }
        [Display(Name = "Description")]
        public string Description { get; set; } = "<p>No description</p>";
        [Display(Name = "Content")]
        public string Content { get; set; } = "None";

        public MarkupString DescriptionAsHtml => new MarkupString(Description);
        public MarkupString LinkAsHtml => new MarkupString($"<a href=\"{Link}\">{Link}</a>");

        public static FeedItem Create(string title, string link, string pubDate, string description)
        {
            string cleanTitle = title.Trim();
            string cleanLink = CleanLink(link);
            DateTime cleanPubDate = string.IsNullOrWhiteSpace(pubDate) ? DateTime.Now : DateTime.Parse(pubDate);
            string cleanDescription = CleanDescription(description);

            return new FeedItem()
            {
                Title = cleanTitle,
                Link = cleanLink,
                PubDate = cleanPubDate,
                Description = cleanDescription,
            };
        }

        public static FeedItem Create(string title, string link, string description)
        {
            string cleanTitle = title.Trim();
            string cleanLink = CleanLink(link);
            string cleanDescription = CleanDescription(description);

            return new FeedItem()
            {
                Title = cleanTitle,
                Link = cleanLink,
                Description = cleanDescription,
            };
        }

        // Get the first paragraph content only.
        public static string CleanDescription(string description)
        {
            string defaultDescription = "<p>No description</p>";

            // 3 cases:
            // 1. description is empty. Return the default description.
            // 2. description has content but no paragraph tags: Return the trimmed description.
            // 3. description has content AND paragraph tags: Capture the content between the first
            // pair of paragraph tags, then trim and return it

            if (string.IsNullOrWhiteSpace(description))
            {
                return defaultDescription;
            }

            string pattern = @"<p>(.*?)</p>";
            var match = Regex.Match(description, pattern, RegexOptions.IgnoreCase);
            return match.Success ? 
                match.Groups[1].Value.Trim() : 
                description.Trim();
        }

        // Get only the link with the protocol, host, and path.
        public static string CleanLink(string link)
        {
            if (string.IsNullOrWhiteSpace(link))
            {
                return string.Empty;
            }

            int hIndex = link.Contains("http") ? link.IndexOf("http") : 0;
            int endLinkIndex = link.Contains("</link>") ? link.IndexOf("</link>") : 0;
            string result = link.Substring(hIndex, link.Length - endLinkIndex);
            return result;
        }
    }

}