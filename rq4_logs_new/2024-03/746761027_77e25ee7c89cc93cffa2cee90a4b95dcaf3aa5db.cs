using Microsoft.EntityFrameworkCore;
using VideoPlatform.WebApp.Data;
using VideoPlatform.WebApp.Data.Entities;

namespace VideoPlatform.WebApp.Repos
{
    public interface ICommentRepository
    {
        IEnumerable<Comment> GetCommentsForVideo(Guid videoId);
        Comment GetCommentById(Guid commentId);
        void AddComment(Comment comment);
        void UpdateComment(Comment comment);
        void DeleteComment(Guid commentId);
    }
    public class CommentRepository:ICommentRepository
    {

        private readonly ApplicationDbContext _context;
        public CommentRepository(ApplicationDbContext context)
        {
            _context = context;
        }
        public IEnumerable<Comment> GetCommentsForVideo(Guid videoId)
        {
            return _context.Comments
                .Where(c => c.VideoId == videoId)
                .ToList();
        }

        public Comment GetCommentById(Guid commentId)
        {
            return _context.Comments
                .FirstOrDefault(c => c.Id == commentId);
        }

        public void AddComment(Comment comment)
        {
            _context.Comments.Add(comment);
            _context.SaveChanges();
        }

        public void UpdateComment(Comment comment)
        {
            _context.Comments.Update(comment);
            _context.SaveChanges();
        }

        public void DeleteComment(Guid commentId)
        {
            var comment = _context.Comments.FirstOrDefault(c => c.Id == commentId);
            if (comment != null)
            {
                _context.Comments.Remove(comment);
                _context.SaveChanges();
            }
        }
    }
}