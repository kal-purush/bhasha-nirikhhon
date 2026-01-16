using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ersProject
{
    public class EmployeeProfile 
    {
        public string userName { get; set; }

        public string passWord { get; set; }
        
        public string fName { get; }
        
        public string lName { get; }

        public bool employee { get; set; }

        public EmployeeProfile(string userName1, string passWord, string fName, string lName, bool employee)
        {
            this.userName = userName1;
            this.passWord = passWord;
            this.fName = fName;
            this.lName = lName;
            this.employee = employee;
        }


        public void submitTicket(int amount, string description)
        {

        }
        
    }
}