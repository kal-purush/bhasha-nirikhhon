using MovieFiles.Core.Interfaces;
using MovieFiles.Core.Models;

namespace MovieFiles.Infrastructure.Repositories
{
    public class CommentRepository : BaseRepository, ICommentRepository
    {
        public CommentRepository(string serverName, string databaseName, string userName, string password) : base(serverName, databaseName, userName, password)
        {
        }

        public Task Comment(Comment comment, int movieId, Guid userId)
        {
            throw new NotImplementedException();
        }

        public Task<List<Comment>> GetComments(int movieId, int page)
        {
            throw new NotImplementedException();
        }
    }
}