using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Scraper
{
    // Article object mirroring a row from the database 
    [Table("articles")]
    public class Article
    {
        [Key]
        public int id { get; set; }

        public string title { get; set; }

        public string body { get; set; }

        public string link { get; set; }

        public DateTime created { get; set; }

    }
}