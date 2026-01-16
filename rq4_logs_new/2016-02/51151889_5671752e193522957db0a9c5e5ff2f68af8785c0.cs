namespace BoardGames.Data.Models
{
    using BoardGames.Data.Common.Models;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;

    public class Comment : BaseModel<int>
    {
        public Comment()
        {
            this.Replies = new HashSet<Reply>();
        }

        [Required]
        public string Content { get; set; }

        [Required]
        public string AuthorId { get; set; }

        public virtual User Author { get; set; }

        [Required]
        public int TutorialId { get; set; }

        public virtual Tutorial Tutorial { get; set; }

        public virtual ICollection<Reply> Replies { get; set; }
    }
}