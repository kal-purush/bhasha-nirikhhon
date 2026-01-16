using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EmployeeApplication
{
    class Employee
    {
        delegate string dispMess(string q);

        int _employeeId;
        string _employeeName;
        string _departmentName;
        string _qualification;
        string FilePath = "C:\\Users\\HP-PC\\Desktop\\log.txt";
        dispMess dm = new dispMess(DispMess);
        
        static string DispMess(string _qualification)
        {
            string _departmentName = "";
            if (String.IsNullOrEmpty(_qualification))
            {
                throw (new InvalidQualificationException(" Exception raised:Qualification field can't be empty."));
            }


            else if (_qualification == "BE" || _qualification == "BCA" || _qualification == "BSC")
            {
                _departmentName = "IT";
                Console.WriteLine("Employee has been added under IT department");
            }

            else if (_qualification == "BCOM" || _qualification == "MCOM" || _qualification == "CA")
            {
                _departmentName = "Accounts";
                Console.WriteLine("Employee has been added under Accounts department");
            }
            else
            {
                Console.WriteLine("Enter proper qualification");
            }
            return _departmentName;
        }
    
        public void add_employee()
        {
            try
            {
                Console.WriteLine("Enter Employee Id: ");
                _employeeId = Convert.ToInt32(Console.ReadLine());
                Console.WriteLine("Enter Employee name: ");
                _employeeName = Console.ReadLine();
                Console.WriteLine("Enter your qualification \n 1. BE 2. BCA 3. BSC 4. BCOM 5. MCOM 6. CA");
                _qualification = Console.ReadLine();
                dm(_qualification);    
                
            }

            catch (InvalidQualificationException qualificationException)
            {
                Console.WriteLine("Invalid Qualification");
                using (System.IO.StreamWriter writer = new StreamWriter(FilePath, true))
                {
                    writer.WriteLine("Message :" + qualificationException.Message + "<br/>" + Environment.NewLine + "StackTrace :" + qualificationException.StackTrace + " " + Environment.NewLine + "Date :" + DateTime.Now.ToString());
                }
            }
            finally
            {
                Console.WriteLine("--Account entered successfully--");
            }



        }
    }
}