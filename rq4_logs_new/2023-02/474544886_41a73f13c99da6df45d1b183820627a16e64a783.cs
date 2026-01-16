using StackExchange.Redis;

namespace Chapter01;

public class Article
{
    public async Task ArticleVoteAsync(IDatabase db, string user, string article)
    {
        // 计算投票截止时间
        var cutoff = GetUnixTime() - Constant.OneWeekInSeconds;
        // 检查是否可以对文章进行投票
        if ((await db.SortedSetScoreAsync("time:", article) ?? 0) < cutoff) return;
        var articleId = article[(article.IndexOf(":") + 1)..];
        if (await db.SetAddAsync("voted:" + articleId, user))
        {
            await db.HashIncrementAsync(article, "votes", 1);
            await db.SortedSetIncrementAsync("score:", article, Constant.VoteScore);
        }
    }

    public async Task PostArticleAsync(IDatabase db, string user, string title, string link)
    {
        var articleId = await db.StringIncrementAsync("article:");
        var votedKey = "voted:" + articleId;
        var articleKey = "article:" + articleId;
        var now = GetUnixTime();

        await db.SetAddAsync(votedKey, user);
        await db.KeyExpireAsync(votedKey, TimeSpan.FromSeconds(Constant.OneWeekInSeconds));

        await db.HashSetAsync(articleKey, new HashEntry[]
        {
        new("poster",user),
        new(nameof(title),title),
        new(nameof(link),link),
        new("time",now),
        new("votes",1)
        });

        await db.SortedSetAddAsync("score:", articleKey, now + Constant.VoteScore);
        await db.SortedSetAddAsync("time:", articleKey, now);
    }

    public async Task<List<Dictionary<RedisValue, RedisValue>>> GetArticleAsync(IDatabase db, int page, string order = "order:")
    {
        var start = (page - 1) * Constant.ArticlePerPage;
        var end = start + Constant.ArticlePerPage - 1;
        var ids = await db.SortedSetRangeByRankAsync(order, start, end);
        var articles = new List<Dictionary<RedisValue, RedisValue>>();
        foreach (var id in ids)
        {
            var article = (await db.HashGetAllAsync(id.ToString())).ToDictionary(x => x.Name, x => x.Value);
            article["id"] = id;
            articles.Add(article);
        }
        return articles;
    }

    public async Task AddRemoveGroupsAsync(IDatabase db, string articleId, string[] toAdd, string[] toRemove)
    {
        var article = "article:" + articleId;
        foreach (var group in toAdd)
        {
            await db.SetAddAsync("group:" + group, article);
        }
        foreach (var group in toRemove)
        {
            await db.SetRemoveAsync("group:" + group, article);
        }
    }

    public async Task<List<Dictionary<RedisValue, RedisValue>>> GetGroupArticlesAsync(IDatabase db, string group, int page, string order = "score:")
    {
        var key = order + group;
        if (!await db.KeyExistsAsync(key))
        {
            await db.SortedSetCombineAndStoreAsync(SetOperation.Intersect, key, "group:" + group, order, Aggregate.Max);
            await db.KeyExpireAsync(key, TimeSpan.FromSeconds(60));
        }
        return await GetArticleAsync(db, page, key);
    }

    private long GetUnixTime() => DateTimeOffset.Now.ToUnixTimeSeconds();
}