using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TeamVoldemort.Employees;

namespace TeamVoldemort.Payroll
{
  class PayRollMenu
  {

    public static int ShowPayRollMenu()
    {
      Console.WriteLine("Pay Roll Menu");
      Console.WriteLine("1. Enter Hours Worked");
      Console.WriteLine("2. Print Checks");
      var userChoice = Int32.Parse(Console.ReadLine());
      return userChoice;
    }

    public static void PayRollActions(int userInput)
    {
      switch (userInput)
      {
        case 1:
          Console.WriteLine("User selected to enter hours worked");
          EmployeeRepository.EnterTime();
          break;
        case 2:
          Console.WriteLine("User selected to print checks");
          EmployeeRepository.PrintChecks();
          break;
        default:
          Console.WriteLine("Invalid Entry");
          break;
      }
    }
  }
}