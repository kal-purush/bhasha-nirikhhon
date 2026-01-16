using SPSS.Entities;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Repository.Entities
{
    public class BlogContent
    {
        [Key]
        public int Id { get; set; }

        [Required]
        [MaxLength(255)]
        public string Title { get; set; } = string.Empty; // Tương ứng với subTitle trong dữ liệu mẫu

        [Required]
        public string Content { get; set; } = string.Empty;

        public string Img { get; set; } = string.Empty;

        [ForeignKey("Blog")]
        public int BlogId { get; set; }

        public virtual Blog? Blog { get; set; }
    }
}