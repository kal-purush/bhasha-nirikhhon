namespace WebApplicationNET6.Model
{
    public class Post
    {
        public int Id { get; set; }
        public int CreateUserId { get; set; }
        public int CategoryId { get; set; }
        public string Title { get; set; } = null!;
        public string? Content { get; set; }
        public DateTime CreateDate { get; set; }
        public DateTime? UpdateDate { get; set; }
        public int? UpdateUserId { get; set; }
    }
}