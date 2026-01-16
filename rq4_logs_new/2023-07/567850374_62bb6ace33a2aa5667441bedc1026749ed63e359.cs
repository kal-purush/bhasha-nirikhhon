using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities
{
    internal class Client
    {
        public string Name { get; set; }
        public string Email { get; set; }

        public Client(string name, string email)
        {
            Name = name;
            Email = email;
        }

        public override bool Equals(object? obj)
        {
            if (obj == null || !(obj is Client)) return false;
            
            Client other = obj as Client; //O que você faz?

            //if(Name.Equals(other.Name) && Email.Equals(other.Email)) return true; //Verifica os dois

            return Email.Equals(other.Email);
        }

        public override int GetHashCode()
        {
            return Email.GetHashCode();
        }
    }
}