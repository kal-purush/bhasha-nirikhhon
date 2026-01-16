using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Wire.WebSite
{
    [APIModule]
    public class PostModule 
    {
        private List<Post> posts = new List<Post> { new Post { Title = "Test", Body = "Body of the test." } };

        public PostModule()
        {
            API.GET("/post/", x => GET());
            API.POST("/post/", x => Create(x.Body.As<Post>()));
        }

        public List<Post> GET()
        {
            return posts;
        }

        public int Create(Post post)
        {
            int id = post.Id;
            posts.Add(post);
            return id;
        }
    }

    public class Post
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public string Body { get; set; }
    }
}