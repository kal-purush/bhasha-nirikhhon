using System;
using System.Collections.Generic;
using System.Text;

namespace EmployeePayrollADONET
{
    public class Employee
    {
        public int EmpId { get; set; }
        public string EmpName { get; set; }
        public string Gender { get; set; }
        public string Phone { get; set; }
        public string EmpAddress { get; set; }
        public string Department { get; set; }
        public string StartDate { get; set; }
        public double BasicPay { get; set; }
        public double Deductions { get; set; }
        public double IncomeTax { get; set; }
        public double NetPay { get; set; }
    }
}