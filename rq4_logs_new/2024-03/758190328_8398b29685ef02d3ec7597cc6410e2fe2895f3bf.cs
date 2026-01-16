namespace Tokengram.Database.Postgres.Entities
{
    public class Comment : BaseEntity
    {
        public long Id { get; set; }

        public string CommenterAddress { get; set; } = null!;

        public long PostId { get; set; }

        public long? ParentCommentId { get; set; }

        public int LikeCount { get; set; } = 0;

        public User Commenter { get; set; } = null!;

        public Post Post { get; set; } = null!;

        public Comment? ParentComment { get; set; }

        public ICollection<CommentLike> Likes { get; set; } = new List<CommentLike>();

        public ICollection<Comment> CommentReplies { get; set; } = new List<Comment>();
    }
}