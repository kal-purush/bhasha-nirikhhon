using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Assignment1
{
    /// <summary>
    /// Contains data to be manipulated by the linked list class
    /// </summary>
    public class Employee:IComparable<Employee>
    {
        public int EmployeeID { get; private set; }
        public string FirstName { get; private set; }
        public string LastName { get; }

        /// <summary>
        /// Constructor to initialize just the employeeID and set the
        /// name attributes to null
        /// </summary>
        /// <param name="employeeId">The unique Id of the employee</param>
        public Employee(int employeeId)
        {
            this.EmployeeID = employeeId;
            this.FirstName = null;
            this.LastName = null;
        }

        /// <summary>
        /// Constructor to create employee with provided employeed Id, first name and last name
        /// </summary>
        /// <param name="employeeId">The unique Id assigned to the employee</param>
        /// <param name="fname">First name of the employee</param>
        /// <param name="lname">Last name of the employee</param>
        public Employee(int employeeId, string fname, string lname)
        {
            this.EmployeeID = employeeId;
            this.FirstName = fname;
            this.LastName = lname;
        }

        /// <summary>
        /// Returns the employee Id of the employee
        /// </summary>
        /// <returns>Returns the employee Id of the employee</returns>
        public int getEmployeeID()
        {
            return this.EmployeeID;
        }

        /// <summary>
        /// Returns the First Name of the employee
        /// </summary>
        /// <returns>Returns the First Name of the employee</returns>
        public string getFirstName()
        {
            return this.FirstName;
        }

        /// <summary>
        /// Returns the Last Name of the employee
        /// </summary>
        /// <returns>Returns the Last Name of the employee</returns>
        public string getLastName()
        {
            return this.LastName;
        }

        /// <summary>
        /// Compares two employees based on their employeeID
        /// </summary>
        /// <param name="emp">Specific employee to compare the current employee against</param>
        /// <returns>The result of comparison</returns>
        public int CompareTo(Employee emp)
        {
            return this.EmployeeID.CompareTo(emp.EmployeeID);
            //if (this.EmployeeID < emp.EmployeeID) return -1;
            //else if (this.EmployeeID > emp.EmployeeID) return 1;
            //else return 0;
        }

        /// <summary>
        /// Prints the employeeId, First Name and Last Name of the employee
        /// </summary>
        /// <returns>The string representing the attributes of the employee</returns>
        public override string ToString()
        {
                if (this.FirstName==null)
                {
                    return getEmployeeID().ToString() + " null null";
                }

                return getEmployeeID().ToString() + " " + getFirstName() + " " + getLastName();                      
        }
    }
}