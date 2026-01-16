using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLogic.Models.DTOs
{
    public class BookDTO
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public string Description { get; set; }
        public int Year { get; set; }
        public byte[] BookContent { get; set; }
        public int AuthorId { get; set; }
        public int GenreId { get; set; }
        public List<int> UserIds { get; set; }
    }
}