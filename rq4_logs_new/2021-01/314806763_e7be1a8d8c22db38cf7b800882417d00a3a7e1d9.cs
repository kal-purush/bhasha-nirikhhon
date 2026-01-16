using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using wypozyczalniaSamochodow;

namespace wypozyczalniaSamochodowTesty
{
    [TestClass]
    public class CarRentalPanelTest
    {
        [TestMethod]
        public void TestMethod1()
        {
            CarRentalPanel rentalPanel = new CarRentalPanel();
            string dateBeforeConversion = "04.05.2020 00:00:00";
            var dateAfterConversion = rentalPanel.changeDateFormat(dateBeforeConversion);
            Assert.AreEqual(dateAfterConversion, "2020-05-04");
        }
    }
}