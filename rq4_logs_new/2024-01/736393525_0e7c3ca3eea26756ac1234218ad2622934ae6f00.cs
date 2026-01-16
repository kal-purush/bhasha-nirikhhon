using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data.Entities
{
    public class Book : BaseEntity
    {
        public string Title { get; set; }
        public string Description { get; set;}
        public int Year { get; set; }
        public byte[] BookContent { get; set; }
        public int AuthorId { get; set; }
        public virtual Author Author { get; set; }
        public int GenreId { get; set; }
        public virtual Genre Genre { get; set; }
        public virtual ICollection<User> Users { get; set; }
    }
}