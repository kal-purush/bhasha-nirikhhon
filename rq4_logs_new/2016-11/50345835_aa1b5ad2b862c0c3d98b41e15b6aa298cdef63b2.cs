using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Affecto.ActiveDirectoryService.Tests
{
    [TestClass]
    public class AccountNameComparerTests
    {
        private AccountNameComparer sut;

        [TestInitialize]
        public void Setup()
        {
            sut = new AccountNameComparer();
        }

        [TestMethod]
        public void NullEqualsNull()
        {
            Assert.IsTrue(sut.Equals(null, null));
        }

        [TestMethod]
        public void ValueDoesNotEqualNull()
        {
            Assert.IsFalse(sut.Equals("value", null));
        }

        [TestMethod]
        public void NullDoesNotEqualValue()
        {
            Assert.IsFalse(sut.Equals(null, "value"));
        }

        [TestMethod]
        public void AccountNameWithoutDomainEqualsAccountNameWithoutDomain()
        {
            Assert.IsTrue(sut.Equals("name", "name"));
        }

        [TestMethod]
        public void AccountNameWithoutDomainEqualsUpperCaseAccountNameWithoutDomain()
        {
            Assert.IsTrue(sut.Equals("name", "NAME"));
        }

        [TestMethod]
        public void UpperCaseAccountNameWithoutDomainEqualsAccountNameWithoutDomain()
        {
            Assert.IsTrue(sut.Equals("NAME", "name"));
        }

        [TestMethod]
        public void AccountNameWithoutDomainDoesNotEqualDifferentAccountNameWithoutDomain()
        {
            Assert.IsFalse(sut.Equals("name", "anothername"));
        }

        [TestMethod]
        public void AccountNameWithDomainEqualsAccountNameWithDomain()
        {
            Assert.IsTrue(sut.Equals("dev\\name", "dev\\name"));
        }

        [TestMethod]
        public void AccountNameWithDomainEqualsUpperCaseAccountNameWithDomain()
        {
            Assert.IsTrue(sut.Equals("dev\\name", "DEV\\NAME"));
        }

        [TestMethod]
        public void UpperCaseAccountNameWithDomainEqualsAccountNameWithDomain()
        {
            Assert.IsTrue(sut.Equals("DEV\\NAME", "dev\\name"));
        }

        [TestMethod]
        public void AccountNameWithDomainDoesNotEqualDifferentAccountNameWithDomain()
        {
            Assert.IsFalse(sut.Equals("dev\\name", "dev\\anothername"));
        }

        [TestMethod]
        public void AccountNameWithDomainEqualsAccountNameWithoutDomain()
        {
            Assert.IsTrue(sut.Equals("dev\\name", "name"));
        }

        [TestMethod]
        public void AccountNameWithDomainEqualsUpperCaseAccountNameWithoutDomain()
        {
            Assert.IsTrue(sut.Equals("dev\\name", "NAME"));
        }

        [TestMethod]
        public void UpperCaseAccountNameWithDomainEqualsAccountNameWithoutDomain()
        {
            Assert.IsTrue(sut.Equals("DEV\\NAME", "name"));
        }

        [TestMethod]
        public void AccountNameWithoutDomainEqualsAccountNameWithDomain()
        {
            Assert.IsTrue(sut.Equals("name", "dev\\name"));
        }

        [TestMethod]
        public void AccountNameWithoutDomainEqualsUpperCaseAccountNameWithDomain()
        {
            Assert.IsTrue(sut.Equals("name", "DEV\\NAME"));
        }

        [TestMethod]
        public void UpperCaseAccountNameWithoutDomainEqualsAccountNameWithDomain()
        {
            Assert.IsTrue(sut.Equals("NAME", "dev\\name"));
        }
    }
}