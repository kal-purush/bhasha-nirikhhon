using System;

namespace Model.AllActors
{
   public class SystemAdministrator : User
   {
        public SystemAdministrator(int id, string username, string password, string name, string surname, string jmbg, DateTime dateOfBirth, string contactNumber, string emailAddress, City city)
            : base(id, username, password, name, surname, jmbg, dateOfBirth, contactNumber, emailAddress, city)
        {
        }

        public SystemAdministrator(string username, string password, string name, string surname, string jmbg, DateTime dateOfBirth, string contactNumber, string emailAddress, City city)
           : base(username, password, name, surname, jmbg, dateOfBirth, contactNumber, emailAddress, city)
        {
        }

        public SystemAdministrator(int id) : base(id)
        {
        }

        public SystemAdministrator()
        {
        }
    }
}