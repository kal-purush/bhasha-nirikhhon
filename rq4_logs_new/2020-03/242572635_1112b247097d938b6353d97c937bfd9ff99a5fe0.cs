using System;

namespace Tholaumuntu.DataAcces.Domain
{
    public class User : Entity
    {
        public int Id { get; set; }
        public string Email { get; set; }
        public string Password { get; set; }
        public string ContactNumber { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime DateOfBirth { get; set; }
        public string Gender { get; set; }
        public string Salt { get; set; }
        public EntityStatus EntityStatus { get; set; }

    }
}