using API.Entities;
using API.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace API.Data
{
    public class PostRepository : IPostRepository
    {
        private readonly DataContext _dataContext;

        public PostRepository(DataContext dataContext)
        {
            _dataContext = dataContext;
        }

        public async Task<IEnumerable<Post>> GetAllPosts()
        {
            return await _dataContext.Posts.ToListAsync();
        } 

        public async Task CreatePost(Post post)
        {
             _dataContext.Posts.Add(post);
             await _dataContext.SaveChangesAsync();
        }
    }
}