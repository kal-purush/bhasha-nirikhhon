using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TeamVoldemort.Employees
{
    class Employee
    {
        public string EmployeeName { get; set; }
        public PositionType EmployeeTitle { get; set; }
        public int EmployeeSales { get; set; }

        public Employee(string employeeName, PositionType employeeTitle, int employeeSales)
        {
            EmployeeName = employeeName;
            EmployeeTitle = employeeTitle;
            EmployeeSales = employeeSales;
        }

    }
}