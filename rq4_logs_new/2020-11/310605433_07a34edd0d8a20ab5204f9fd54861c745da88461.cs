using NUnit.Framework;
using Payroll_Service_Ado;

namespace TestEmployeePayRoll
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
            
        }
        /// <summary>
        /// This method is check whether database connection is established or not
        /// </summary>
        [Test]
        public void TestConnection()
        {
            PayrollRepository payrollRepository = new PayrollRepository();
            bool expected = true;
            bool actual=payrollRepository.CheckConnection();
            Assert.AreEqual(expected, actual);
        }
        /// <summary>
        /// This method checks whether the table is updated or not
        /// Here 1 represents one row is effected in table
        /// </summary>
        [Test]
        public void TestUpdate()
        {
            PayrollRepository payrollRepository = new PayrollRepository();
            int expected = 1;
            int actual = payrollRepository.UpdateSalary();
            Assert.AreEqual(expected, actual);
        }
        /// <summary>
        /// This Method Adds the employee.
        /// </summary>
        [Test]
        public void AddEmployee()
        {
            // Arrange
            EmployeeDetails employeeDetails = new EmployeeDetails();
            employeeDetails.gender = 'M';
            employeeDetails.empName = "Sam";
            employeeDetails.phoneNumber = "4578547896";
            employeeDetails.startDate = new System.DateTime(2019, 08, 24);
            PayrollDetails payroll = new PayrollDetails { BasePay = 80000, Deductions = 10000, IncomeTax = 3000,TaxablePay=5000 };

            // Act
            PayrollRepository payrollRepository = new PayrollRepository();
            bool actual = payrollRepository.AddEmployee(employeeDetails,payroll);
            bool expected = true;

            // Assert
            Assert.AreEqual(expected, actual);
        }
    }
}