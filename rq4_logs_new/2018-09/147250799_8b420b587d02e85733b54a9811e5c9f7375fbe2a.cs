using System;

namespace StackOverflowPost
{
    public class StackOverflowPost
    {
        private int _totalVotes;

        public string Title { get; set; }
        public string Description { get; set; }
        public DateTime DateTimeCreated { get; set; }

        public void UpVote()
        {
            _totalVotes += 1;
        }

        public void DownVote()
        {
            _totalVotes -= 1;
        }

        public void displayPost()
        {
            Console.WriteLine(
                $@"Title: {Title}
Description: {Description}
Date Created: {DateTimeCreated}
Total Votes: {_totalVotes}"
                );
        }
    }
}