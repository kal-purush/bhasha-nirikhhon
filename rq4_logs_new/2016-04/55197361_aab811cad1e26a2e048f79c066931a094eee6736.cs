using DataLayer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLayer.ViewModels
{
    public class EmployeeVM
    {
        public int ID { get; set; }
        public string UserName { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }

        public static List<EmployeeVM> GetEmployees()
        {
            return EmployeesToVM(DataAccessObject.Instance.GetEmployees());
        }

        public static bool AuthenticateEmployee(int employeeId, string password)
        {
            return DataAccessObject.Instance.AuthenticateEmployee(employeeId, password);
        }

        private static List<EmployeeVM> EmployeesToVM(List<User> users)
        {
            List<EmployeeVM> returndata = new List<EmployeeVM>();
            foreach(User u in users)
            {
                returndata.Add(new EmployeeVM
                {
                    ID = u.ID,
                    UserName = u.UserName,
                    FirstName = u.FirstName,
                    LastName = u.LastName
                });
            }
            return returndata;
        }

    }
}