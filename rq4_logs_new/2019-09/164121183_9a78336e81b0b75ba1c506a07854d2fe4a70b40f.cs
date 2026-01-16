namespace SFA.DAS.RoATPService.Application.UnitTests
{
    using NUnit.Framework;
    using Validators;

    [TestFixture]
    public class OrganisationValidationIsValidCompanyNumberTests
    {
        private OrganisationValidator _validator;

        [SetUp]
        public void Before_each_test()
        {
            _validator = new OrganisationValidator(null,null,null);
        }

        [TestCase("", true)]
        [TestCase(" ", true)]
        [TestCase(null, true)]
        [TestCase("  ", true)]
        [TestCase("A", false)]
        [TestCase("11", false)]
        [TestCase("1 ", false)]
        [TestCase("AA", false)]
        [TestCase("ABC", false)]
        [TestCase("1234567",false)]
        [TestCase("012345678",false)]
        [TestCase("1000$!&*^%",false)]
        [TestCase("AB123456", true)]
        [TestCase("AB12345C", true)]
        [TestCase("12345678", true)]
        [TestCase("ac3456CD", true)]
        public void Company_Number_Validation_checks(string companyNumber, bool acceptedValue)
        {
              var result = _validator.IsValidCompanyNumber(companyNumber);
            Assert.AreEqual(result,acceptedValue);
        }    
    }
}