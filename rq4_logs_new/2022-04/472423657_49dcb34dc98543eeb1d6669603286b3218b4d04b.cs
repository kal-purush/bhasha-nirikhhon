using System.ComponentModel.DataAnnotations;

namespace Data.Models
{
    public class Chat
    {
        [Key]
        public string Id { get; set; }

        public string? Title { get; set; }

        public bool IsGroup { get; set; }

        public string? CreatorId { get; set; }

        public virtual User? Creator { get; set; }

        public virtual ICollection<Message> Messages { get; set; }

        public virtual ICollection<User> Users { get; set; }

    }
}