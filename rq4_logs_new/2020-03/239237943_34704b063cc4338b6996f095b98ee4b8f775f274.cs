using NUnit.Framework;
using PlaySlip.Application;

namespace PaySlip.UnitTests
{

    public class AnnualSalaryTests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test] 
        public void IsValid_InvalidAnnualSalary_ReturnsFalse()
        {
            var annualSalaryValidator = new AnnualSalaryValidator();
        
            var annualSalary = "-100";
        
            var result = annualSalaryValidator.IsValid(annualSalary);
            
            Assert.AreEqual( false, result);
        }
        
        [Test] 
        public void IsValid_ValidAnnualSalary_ReturnsTrue()
        {
            var annualSalaryValidator = new AnnualSalaryValidator();
        
            var annualSalary = "60000";
        
            var result = annualSalaryValidator.IsValid(annualSalary);
            
            Assert.AreEqual( true, result);
        }

        
    }
}