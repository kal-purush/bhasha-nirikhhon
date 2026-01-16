using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using wypozyczalniaSamochodow;
using System.Windows.Forms;

namespace wypozyczalniaSamochodowTesty
{
    [TestClass]
    public class ReservationPanelTest
    {
        [TestMethod]
        public void checkSetParentMethod()
        {
            var app = new App();
            var registration = new RegistrationPanel();
            registration.setParent(app);
            Assert.AreEqual(app, registration.parent);
        }

        //(firstName, lastName, city, address, houseNoumber, apartmentNumber, email, password, password2)

        [TestMethod]
        public void checkFirstNameExisting()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("","Hymel","Lipinki Lozyckie","Laczna","43","1","aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkFirstNameIsNotDigit()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga2", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "1", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkLastNameExisting()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "", "Lipinki Lozyckie", "Laczna", "43", "1", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkLastNameIsNotDigit()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel2", "Lipinki Lozyckie", "Laczna", "43", "1", "aaa@bbb.com", "123456", "123456"));
        }


        [TestMethod]
        public void checkCityExisting()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "", "Laczna", "43", "1", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkCityIsNotDigit()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie5", "Laczna", "43", "1", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkAddressExisting()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "", "43", "1", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkAdressIsNotDigit()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna 43", "43", "1", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkHouseNumberExisting()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "", "1", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkHouseNumberIsDigit()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "aaa", "1", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkApartnumberExisting()
        {
            var registration = new RegistrationPanel();
            Assert.IsTrue(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkApartNumberIsDigit()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "aa", "aaa@bbb.com", "123456", "123456"));
        }

        [TestMethod]
        public void checkEmailExisting()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "1", "", "123456", "123456"));
        }



        [TestMethod]
        public void checkDataValidation()
        {
            var registration = new RegistrationPanel();
            Assert.IsTrue(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "1", "aaa@bbb.com", "1234567", "1234567"));
        }

        [TestMethod]
        public void checkDataValidation_DifferentPasswords()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "1", "aaa@bbb.com", "12345678", "1234567"));
        }

        [TestMethod]
        public void checkDataValidation_TooShortPassword()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "1", "aaa@bbb.com", "12345", "12345"));
        }

        [TestMethod]
        public void checkDataValidation_IncorrectEmail()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "1", "aaabbb.com", "12345678", "12345678"));
        }

        [TestMethod]
        public void checkDataValidation_IncorrectEmail2()
        {
            var registration = new RegistrationPanel();
            Assert.IsFalse(registration.checkCorrectness("Jadwiga", "Hymel", "Lipinki Lozyckie", "Laczna", "43", "1", "aaa@bbbcom", "12345678", "12345678"));
        }

    }
}