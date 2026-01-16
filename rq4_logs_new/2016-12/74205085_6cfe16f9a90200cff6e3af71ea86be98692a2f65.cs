using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Web;

namespace BookStore.Migrations.Entities
{
    [Table("Images")]
    public class Image
    {
        [Key]
        public int ImageId { get; set; }
        [Required]
        public string Path { get; set; }
        public virtual Book Book { get; set; }
    }
}