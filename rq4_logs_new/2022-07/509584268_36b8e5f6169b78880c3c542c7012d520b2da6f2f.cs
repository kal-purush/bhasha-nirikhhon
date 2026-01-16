using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BL.DTOs
{
    public class BookDto
    {
        public Guid BookId { get; set; }
        public string Genre { get; set; }
        public string Title { get; set; }
        public string Author { get; set; }
        public int Price { get; set; }
        public IEnumerable<BookRevisionDto> BookRevisions { get; set; }
    }
}