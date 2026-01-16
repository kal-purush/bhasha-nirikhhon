using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace hackernewsapi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class HackerNewsController : ControllerBase
    {
        private readonly ILogger<HackerNewsController> _logger;
        private const string BEST_STORIES_URL = "Https://hacker-news.firebaseio.com/v0/beststories.json";
        private const string STORY_DETAIL_URL = "https://hacker-news.firebaseio.com/v0/item/";

        public HackerNewsController(ILogger<HackerNewsController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public async Task<IEnumerable<Story>> Get()
        {
            var client = new HttpClient();
            var stories = new List<Story>();

            var responseBestStories = await client.GetStringAsync(BEST_STORIES_URL);
            var bestStories = JsonSerializer.Deserialize<int[]>(responseBestStories);

            Array.Sort(bestStories);

            foreach(var currentStory in bestStories){ 
                var responseStory = await client.GetStringAsync($"{STORY_DETAIL_URL}{currentStory}.json");   
                var story = JsonSerializer.Deserialize<Story>(responseStory);

                stories.Add(story);
            }

            return stories.OrderByDescending(s => s.score).Take(20);
        }
    }
}