using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinalProjectLibraryManagerV01E.Models
{
    public interface IUser
    {
        public string Name { get; set; }
        public int ID { get; set; }
        public string Password { get; set; }
        public bool IsStudent {  get; set; }
        public List<Book> books { get; set; }
        public bool IsFined { get; set; }
        public bool HasBorrowed { get; set; }
        public int TotalFine { get; set; }
        
    }
}