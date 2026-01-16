using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProjectManagementSystem.DataAccessLayer.Repository;
using ProjectManagementSystem.Model;

namespace ProjectManagementSystem.DataAccessLayer.Tests
{
    [TestClass]
    public class UserRepositoryTest
    {
        private const string FIRST_NAME = "John";
        private const string LAST_MAME = "Rodrigez";
        private const string PATRONYMIC = "Johnson";
        private const string EMAIL = "john.rodrigez@gmail.com";
        private const string PHONE_NUMBER = "8746512831";
        private const string POSITION = "Sheep";

        private static int count;

        private UnitOfWork uow = new UnitOfWork("UnitTestDB");

        [TestMethod]
        public void TestGetAll()
        {
            int firstCount = uow.Users.GetAll().Count();
            SaveUser();
            Assert.AreEqual(firstCount + 1, uow.Users.GetAll().Count());
        }

        [TestMethod]
        public void TestAdd()
        {
            User u = SaveUser();
            Assert.IsNotNull(uow.Users.Get(u.Id));
        }

        [TestMethod]
        public void TestUpdate()
        {
            User u = SaveUser();
            u.Role = UserRole.PROJECT_MANAGER;
            uow.Users.Update(u);
            uow.Save();
            Assert.AreEqual(u, uow.Users.Get(u.Id));
        }

        [TestMethod]
        public void TestDelete()
        {
            User u = SaveUser();
            uow.Users.Delete(u.Id);
            uow.Save();
            Assert.IsNull(uow.Users.Get(u.Id));
        }

        private User SaveUser()
        {
            count++;
            User u = new User()
            {
                FirstName = FIRST_NAME + count,
                LastName = LAST_MAME + count,
                Patronymic = PATRONYMIC + count,

                Login = count.ToString(),
                Password = count.ToString(),

                Email = EMAIL,
                PhoneNumber = PHONE_NUMBER,
                Position = POSITION
            };
            uow.Users.Add(u);
            uow.Save();
            return u;
        }
    }
}