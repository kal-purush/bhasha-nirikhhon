namespace Mastodon.Data;

[BsonIgnoreExtraElements]
public sealed class Poll
{
    [BsonId]
    [BsonRequired]
    [BsonElement("_id")]
    public required string StatusId;
    
    [BsonRequired]
    public required PollKind Kind;

    [BsonRequired]
    public required Dictionary<string, Vote> Votes;
}

[BsonIgnoreExtraElements]
public sealed class Vote
{
    public required string UserId;
    public required List<VoteChoice> Choices;
}

[BsonIgnoreExtraElements]
public sealed class VoteChoice
{
    public required uint Choice;
    public required uint Weight;
}