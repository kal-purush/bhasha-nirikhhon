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
    }
}