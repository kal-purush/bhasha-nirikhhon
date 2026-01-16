using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RAD_Project.BasicClasses
{
    class Admin : User
    {
        public Admin() { user_Type = "Admin"; }
        private string adminPrivilage = null;

        public void acceptUser(User user) { throw new NotImplementedException(); }   
        public void rejectUser(User user) { throw new NotImplementedException(); }
        public void acceptAppoinment(Appoinment appoinment) { throw new NotImplementedException(); }
        public void rejectAppoinment(Appoinment appoinment) { throw new NotImplementedException(); }

    }
}