using System.Diagnostics;
using System.Text.RegularExpressions;
using StackExchange.Redis;
using Xunit;

namespace Chapter01;

public partial class ArticleTests
{
    [Fact]
    public async Task Artile_Test()
    {
        var redis = await ConnectionMultiplexer.ConnectAsync("localhost", opts =>
        {
            opts.AllowAdmin = true;
        });
        var db = redis.GetDatabase(1);
        var article = new Article();
        var articleId = await article.PostArticleAsync(db, "user1", "title1", "http://redis.io");
        var exist = await db.HashExistsAsync("article:" + articleId, "title");
        Assert.True(exist);

        //ArticleVoteAsync
        await article.ArticleVoteAsync(db, "other_user1", "article:" + articleId);
        var voted = await db.HashGetAsync("article:" + articleId, "votes");
        Assert.True((int)voted > 1);

        // GetArticleAsync
        var articles = await article.GetArticleAsync(db, 1);
        Assert.True(articles.Any());

        // AddRemoveGroupsAsync and GetGroupArticlesAsync
        await article.AddRemoveGroupsAsync(db, articleId.ToString(), new[] { "new-group" }, Array.Empty<string>());
        var groupedArticles = await article.GetGroupArticlesAsync(db, "new-group", 1);
        Assert.True(groupedArticles.Any());

        await ResetAsync(redis, db);
    }


    /// <summary>
    /// <see href="https://github.com/StackExchange/StackExchange.Redis/issues/1526#issuecomment-655935474"/>
    /// </summary>
    /// <param name="redis"></param>
    /// <param name="db"></param>
    /// <returns></returns>
    private async Task ResetAsync(ConnectionMultiplexer redis, IDatabase db)
    {
        var server = redis.GetServer("localhost", 6379);
        var batch = new List<RedisKey>();
        var keyRegex = MyRegex();
        await foreach (var key in server.KeysAsync(1))
        {
            if (keyRegex.IsMatch(key.ToString()))
            {
                batch.Add(key);
            }
        }

        await db.KeyDeleteAsync(batch.ToArray());
    }

    [GeneratedRegex("time:.*|voted:.*|score:.*|article:.*|group:.*")]
    private static partial Regex MyRegex();
}