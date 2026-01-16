using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace InstagramProject.Models
{
    public class PostManagement
    {
        User User { get; set; }



        private readonly InstagramContext _context;
        private readonly User _currentUser;




        public PostManagement(InstagramContext context, User user)
        {
            if (context == null)
            {
                throw new ArgumentNullException(nameof(context), "Database context cannot be null.");
            }

            _context = context;
            _currentUser = user;

        }


        // Method to create a post with user input
        public void CreatePostFromUserInput()
        {
            // Prompt the user for the picture URL
            Console.WriteLine("Enter the picture URL:");
            string imageUrl = Console.ReadLine()!;

            // Prompt the user for the caption
            Console.WriteLine("Enter the caption:");
            string caption = Console.ReadLine()!;

            // Set the current timestamp
            DateTime currentTimestamp = DateTime.Now;

            // Create a new post object
            var newPost = new Post
            {
                Image = imageUrl,
                Caption = caption,
                Timestamp = currentTimestamp,
                UserId = _currentUser.UserId // Example UserId; you can replace it with the current user's ID
            };

            // Save the post to the database
            CreatePost(newPost);
        }

        // Create a new post
        public void CreatePost(Post newPost)
        {
            _context.Posts.Add(newPost);
            _context.SaveChanges();
            Console.WriteLine($"Post created with ID: {newPost.PostId}");
        }


        public void DisplayAllPosts()
        {

            using (var realDatabase = new InstagramContext())
            {
                _context.Database.EnsureCreated();



                var posts = _context.Posts.ToList();

                Console.WriteLine("📢 All Posts in Database:");
                foreach (var post in posts)
                {
                    Console.WriteLine($"[{post.PostId}] 🖼 {post.Image} - {post.Caption} ({post.Timestamp})");
                }
            }
        }
    }
}