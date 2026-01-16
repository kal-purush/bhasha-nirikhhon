using System;

namespace StackOverflowPost
{
    public class Post
    {
        public string Title { get; private set; }
       
        public string Description { get; private set; }
        
        public DateTime DateCreated { get; }
        
        public int CurrentVotes { get; private set; }

        public Post(string title, string description)
        {
            Title = title;
            
            Description = description;
            
            DateCreated = DateTime.UtcNow;
        }

        public void UpVote()
        {
            CurrentVotes += 1;
        }

        public void DownVote()
        {
            CurrentVotes -= 1;
        }
        
    }
}