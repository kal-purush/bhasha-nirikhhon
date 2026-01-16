

namespace ExploreGetRssFeed.Data
{
    public class DbInitializer
    {
        public static void Initialize(ExploreGetRssFeedContext context)
        {
            context.Database.EnsureCreated();
            
            // Look for any feed entries.
            if (context.FeedEntryDataModels.Any())
            {
                return;   // DB has been seeded
            }
            var feedEntries = new FeedEntryDataModel[]
            {
                new() {Title="Dev Blogs .Net Feed",WebAddress="https://devblogs.microsoft.com/dotnet/feed/",RouteName="DevBlogsDotNetFeed"},
                new() {Title="NCEI NOAA Feed",WebAddress="https://www.ncei.noaa.gov/news.xml",RouteName="NceiNoaaGov"},
                new() {Title="Aspireify Feed",WebAddress="https://aspireify.net/rss",RouteName="Aspireify"},
                new() {Title="Test Broken Feed",WebAddress="https://localhost:8080/broken",RouteName="ErrorFeed"},
            };
            //foreach (FeedEntryDataModel s in feedEntries)
            //{
            //    context.FeedEntryDataModels.Add(s);
            //}
            context.FeedEntryDataModels.AddRange(feedEntries);

            context.SaveChanges();
        }
    }
}