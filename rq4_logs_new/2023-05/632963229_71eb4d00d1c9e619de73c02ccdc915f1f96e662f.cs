using MovieFiles.Core.Models;
using MovieFiles.Core.Models.Activity;

namespace MovieFiles.Core.Interfaces
{
    public class CommentService : ICommentService
    {
        private readonly ICommentRepository _commentRepository;
        private readonly IActivityRepository _activityRepository;

        public CommentService(ICommentRepository commentRepository, IActivityRepository activityRepository)
        {
            _commentRepository = commentRepository;
            _activityRepository = activityRepository;
        }
        public async Task Comment(Comment comment, int movieId, Guid userId)
        {
            await _commentRepository.Comment(comment, movieId, userId);

            var commentActivity = CreateCommentActivity(comment, movieId, userId);

            await _activityRepository.AddActivity(commentActivity);

        }

        private CommentActivity CreateCommentActivity(Comment comment, int movieId, Guid userId)
        {
            return new CommentActivity()
            {
                Created = DateTime.Now,
                MovieId = movieId,
                UserId = userId,
                CommentText = comment.Text
            };

        }
        public async Task<List<Comment>> GetComments(int movieId, int page)
        {
            return await _commentRepository.GetComments(movieId, page);
        }
    }
}