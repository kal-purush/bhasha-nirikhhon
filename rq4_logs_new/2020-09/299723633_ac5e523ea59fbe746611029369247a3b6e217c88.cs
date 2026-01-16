using System;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

namespace hackernewsapi.Services{
    public class HackerNewsService : IHackerNewsService
    {
        private const string BEST_STORIES_URL = "Https://hacker-news.firebaseio.com/v0/beststories.json";
        private const string STORY_DETAIL_URL = "https://hacker-news.firebaseio.com/v0/item/";

        public async Task<int[]> GetBestStoryIds()
        {
            using (var client = new HttpClient())
            {
                var responseBestStories = await client.GetStringAsync(BEST_STORIES_URL);
                var bestStories = JsonSerializer.Deserialize<int[]>(responseBestStories);
                Array.Sort(bestStories);
                return bestStories;
            }
        }

        public async Task<Story> GetStoryFromId(int storyId)
        {
            using (var client = new HttpClient())
            {
                var url = $"{STORY_DETAIL_URL}{storyId}.json";
                var responseBestStories = await client.GetStringAsync(url);
                Story story = JsonSerializer.Deserialize<Story>(responseBestStories);
                return story;
            }
        }
    }

}