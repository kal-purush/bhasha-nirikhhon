using NUnit.Framework;

namespace Account.Test
{
    public class Tests
    {
        private Account[] _accounts =
        {
            new Account("Leonardo", 0.00),
            new Account("Mickelangelo", 120.00),
            new Account("Raphael", 20.00),
            new Account("VanGogh", 1300.00)
        };

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Account_CheckNameGetSetMethods()
        {
            _accounts[0].Name = "Leonardo Francheskovich";
            Assert.AreEqual(_accounts[0].Name, "Leonardo Francheskovich", "Name entry setter has not been saved correctly");
        }

        [Test]
        public void Account_CheckSetupEntryGetMethods()
        {
            Assert.AreEqual(_accounts[2].Name, "Raphael", "Entry has not been saved correctly");
            Assert.AreEqual(_accounts[2].Balance(), 20, 0.001, "Entry has not been saved correctly");
        }

        [Test]
        public void Account_CheckDepositMethod()
        {
            _accounts[1].Deposit(30);
            Assert.AreEqual(_accounts[1].Balance(), 150, 0.001, "Deposit does not add to balance in correct way");
        }

        [Test]
        public void Account_CheckWithdrawalMethod()
        {
            _accounts[1].Withdrawal(100);
            Assert.AreEqual(_accounts[1].Balance(), 40, 0.001, "Withdrawal does not substract from balance in correct way");
        }

        [Test]
        public void Account_CheckToStringMethod()
        {
            Assert.AreEqual(_accounts[2].ToString(), "Account: Raphael; Balance: 20 EUR", "Report /ToString()/ does not return correct data");
        }

        [Test]
        public void Account_CheckInternalDepositWithdrawal()
        {
            _accounts[0].Deposit(_accounts[1].Withdrawal(10));
            Assert.AreEqual(_accounts[1].Balance(), 140, 0.001, "Internal Deposit / Withdrawal does not work in correct way");
            Assert.AreEqual(_accounts[0].Balance(), 10, 0.001, "Internal Deposit / Withdrawal does not work in correct way");
        }      
    }   
}