using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CoolBytes.Core.Extensions;

namespace CoolBytes.Core.Models
{
    public class User
    {
        public int UserId { get; private set; }
        public string Identifier { get; private set; }

        public User(string identifier)
        {
            identifier.IsNotNullOrWhiteSpace();

            Identifier = identifier;
        }

        private User()
        {
            
        }
    }
}