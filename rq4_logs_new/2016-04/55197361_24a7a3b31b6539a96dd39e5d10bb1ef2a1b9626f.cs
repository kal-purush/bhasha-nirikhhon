using BusinessLayer.ViewModels;
using DataLayer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLayer.BusinessLogic
{
    public class EmployeeLogic
    {
        public static List<EmployeeVM> GetEmployees()
        {
            return EmployeesToVM(DataAccessObject.Instance.GetEmployees());
        }

        public static bool AuthenticateEmployee(int employeeId, string password)
        {
            return DataAccessObject.Instance.AuthenticateEmployee(employeeId, password);
        }

        public static bool AuthenticateEmployee(string employeeUserName, string password)
        {
            return DataAccessObject.Instance.AuthenticateEmployee(employeeUserName, password);
        }

        private static List<EmployeeVM> EmployeesToVM(List<User> users)
        {
            List<EmployeeVM> returndata = new List<EmployeeVM>();
            foreach (User u in users)
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