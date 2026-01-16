using iPhoneBE.Data.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace iPhoneBE.Data.Entities
{
    public class Blog
    {
        [Key]
        public int BlogId { get; set; }

        [Required]
        public string Title { get; set; }

        [Required]
        public string Content { get; set; }

        [Required]
        public string Author { get; set; }

        [ForeignKey("ProductId")]
        public int ProductId { get; set; }

        public DateTime UploadDate { get; set; }

        public DateTime UpdateDate { get; set; }

        public int View { get; set; }

        public int Like { get; set; }

        public bool IsActive { get; set; }

        public Product Product { get; set; }
        public virtual ICollection<UserBlogView> UserBlogViews { get; set; }
        public virtual ICollection<BlogImage> BlogImages { get; set; }
    }
}