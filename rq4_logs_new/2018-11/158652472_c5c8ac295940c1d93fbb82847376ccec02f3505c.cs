namespace SSC_PoC2.Models
{
    public class NewsArticle: Sitecore.Services.Core.Model.EntityIdentity
    {
        public string Title { get; set; }
        public string Description { get; set; }
    }
}