using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinalProjectLibraryManagerV01E.Models
{
    public class Librarian
    {
        public string Name { get; set; }
        public string ID { get; set; }
        string passsword;
        public string Password { get { return Password; } }
        public Librarian(string name, string id, string password)
        {
            this.Name = name;
            this.ID = id;
            this.passsword = password;
        }
    }
}