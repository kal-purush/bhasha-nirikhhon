using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace XabluApp.Core
{
    public class BlogService : IBlogService
    {
        private IBlogClient blogClient { get; }
        public BlogService(IBlogClient blogClient)
        {
            this.blogClient = blogClient;
        }

        public async Task<BlogModel> GetBlog(int id)
        {
            return await blogClient.GetBlog(id);
        }

        public async Task<IEnumerable<BlogModel>> GetBlogs()
        {
            return await blogClient.GetBlogs();
        }
    }
}