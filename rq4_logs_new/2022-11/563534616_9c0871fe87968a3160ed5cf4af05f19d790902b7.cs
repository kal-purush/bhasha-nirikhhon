using DAL;
using DAL.Entities;
using Microsoft.EntityFrameworkCore;

namespace BLL
{
    public class PostService : IPost
    {
        private readonly AppDbContext _context;
        public PostService(AppDbContext context)
        {
            _context = context;
        }

        public Task Create(Post post)
        {
            throw new NotImplementedException();
        }

        public Task Delete(int id)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Post> GetAll()
        {
            return _context.Posts.Include(post => post.PostReplies);
        }

        public Post GetById(int id)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Post> GetPostsByTopic(int id, int pageNumber, int pageSize)
        {
            return _context.Topics
                .Where(topic => topic.Id == id)
                .First()
                .Posts
                .OrderBy(x => x.Id) //and(!) date
                .Skip((pageNumber - 1) * pageSize)
                .Take(pageSize);
        }

        public IEnumerable<Post> GetPostsByTopic(int id)
        {
            return _context.Topics
                .Where(topic => topic.Id == id)
                .First()
                .Posts
                .OrderBy(x => x.Id); //and(!) date
        }

        public IEnumerable<Post> GetPostsGlobalSearch(string searchString)
        {
            return _context.Posts
                .Where(p => p.Description.Contains(searchString)||
                p.Content.Contains(searchString));
        }

        public IEnumerable<Post> GetPostsGlobalSearch(string searchString, int pageNumber, int pageSize)
        {
            return _context.Posts
                .Where(p => p.Description.Contains(searchString)|| 
                p.Content.Contains(searchString))
                .OrderBy(x => x.Id) //and(!) date
                .Skip((pageNumber - 1) * pageSize)
                .Take(pageSize); ;
        }

        public IEnumerable<Post> GetPostsInTopicSearch(int id, string searchString)
        {
            return _context.Posts.Where(
                p => p.TopicId == id &&
                (p.Description.Contains(searchString) ||
                p.Content.Contains(searchString)))
                .OrderBy(x => x.Id); //and(!) date
        }
        public IEnumerable<Post> GetPostsInTopicSearch(int id, string searchString, int pageNumber, int pageSize)
        {
            return _context.Posts.Where( 
                p => p.TopicId == id &&
                (p.Description.Contains(searchString) ||
                p.Content.Contains(searchString)))
                .OrderBy(x => x.Id) //and(!) date
                .Skip((pageNumber - 1) * pageSize)
                .Take(pageSize);
        }

        public Task UpdatePostContent(int id, string newContent)
        {
            throw new NotImplementedException();
        }

        public Task UpdatePostDescription(int id, string newDescription)
        {
            throw new NotImplementedException();
        }
    }
}