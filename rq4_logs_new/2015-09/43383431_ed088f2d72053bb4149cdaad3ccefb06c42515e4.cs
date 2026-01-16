using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace asp_mvc_test.Models
{
    public class EmployeeBusinessLayer
    {
        public List<Employee> GetEmployees()
        {
            List<Employee> le = new List<Employee>();

            le.Add(new Employee("Boris", "Kavin", 35000));
            le.Add(new Employee("Dari", "Kavin", 30000));
            le.Add(new Employee("ignat", "Kavin", 5000));
            le.Add(new Employee("Georgiy", "Kavin", 6000));
            le.Add(new Employee("Bis", "Kavin", 40000));
            le.Add(new Employee("Boris", "Kavin", 3000));
            le.Add(new Employee("Boris", "Kavin", 35000));

            return le;
        }
    }
}