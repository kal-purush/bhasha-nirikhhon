using NUnit.Framework;

namespace Exercise8.Test
{
    public class Tests
    {
        private Employee _testEmployee;
        private string _expected;
        private double _expectedSalary;

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Employee_LowPaidEmployee()
        {
            _testEmployee = new Employee("Mister Jecabson", 7.50, 35);
            string message = _testEmployee.SalaryPayingMessage();
            _expected = "Error! Not less than 8.00 USD/h are allowed according to US legislation!";
            Assert.AreEqual(_expected, message, "Error! Not less than 8.00 USD/h are allowed according to US legislation! is expected");
        }

        [Test]
        public void Employee_NormalEmployee()
        {
            _testEmployee = new Employee("Mister Jecabson", 8.0, 35);
            string message = _testEmployee.SalaryPayingMessage();
            _expected = "To be paid in standard order";
            Assert.AreEqual(_expected, message, "To be paid in standard order is expected");
        }

        [Test]
        public void Employee_OverWorkingEmployee()
        {
            _testEmployee = new Employee("Mister Jecabson", 8.50, 70);
            string message = _testEmployee.SalaryPayingMessage();
            _expected = "Error! More than 60 hours worked!";
            Assert.AreEqual(_expected, message, "Error! More than 60 hours worked! is expected");
        }

        [Test]
        public void Employee_StandardWorkTimeSalary()
        {
            _testEmployee = new Employee("Mister Jecabson", 8.50, 30);
            double salary = _testEmployee.GetSalary();
            _expectedSalary = 255;
            Assert.AreEqual(_expectedSalary, salary, 0.001, "255 USD is to be paid for 30h of 8.50 USD/h employee");
        }

        [Test]
        public void Employee_OverTimeWorkTimeSalary()
        {
            _testEmployee = new Employee("Mister Jecabson", 8.50, 60);
            double salary = _testEmployee.GetSalary();
            _expectedSalary = 595;
            Assert.AreEqual(_expectedSalary, salary, 0.001, "595 USD is to be paid for 40h of 8.50 USD/h and 20 h overtime for employee");
        }
    }
}