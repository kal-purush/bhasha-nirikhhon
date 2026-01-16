using static System.String;

namespace FakeNews.Domain.Models;

public class Post : BaseEntity
{
    public string Title { get; init; } = Empty;
    public string Text { get; init; } = Empty;
    public PostStatus PostStatus { get; set; } = PostStatus.Hidden;
    public Link Resource { get; init; } = null!;
    public ICollection<Link> Links { get; protected set; } = new List<Link>();
    public PostTags PostTags { get; init; } = null!;
    
    private Post() {}
    
    public static Post Create(string title, string text, PostStatus postStatus, Link resource, ICollection<Link> links, PostTags postTags)
    {
        return new Post()
        {
            Id = Guid.NewGuid(),
            CreateDate = DateTime.Now,
            Title = title,
            Text = text,
            PostStatus = postStatus,
            Resource = resource,
            Links = links,
            PostTags = postTags
        };
    }
}