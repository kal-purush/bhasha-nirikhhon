using System.ComponentModel;

namespace GroupRareAPI.Models
{
    public class Post
    {
        public int Id { get; set; }
        public int RareUserId { get; set; }
        public RareUser RareUser {  get; set; }
        public int CategoryId { get; set; }
        public Category Category { get; set; }
        public string Title { get; set; }
        public DateTime PublicationDate {  get; set; }
        public string ImageUrl { get; set; }
        public string Content { get; set; }
        public bool Approved { get; set; }
        public List<Reaction> Reactions { get; set; }
        public List<Tag> Tags { get; set; }
    }
}