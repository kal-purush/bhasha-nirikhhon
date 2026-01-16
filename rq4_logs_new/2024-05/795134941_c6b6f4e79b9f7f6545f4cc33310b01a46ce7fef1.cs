using ClassLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Testing1
{
    [TestClass]
    public class tstStaff
    {
        [TestMethod]
        public void InstanceOK()
        {
            clsStaff AStaff = new clsStaff();
            Assert.IsNotNull(AStaff);
        }
        public void StaffIDOK()
        {
            // Create an instance class
            clsStaff AStaff = new clsStaff();
            // Create test data
            int testData = 1;
            AStaff.StaffId = testData;
            Assert.AreEqual(AStaff.StaffId, testData);
        }
        public void StaffNameOK()
        {
            // Create an instance class
            clsStaff AStaff = new clsStaff();
            // Create test data
            string testData = "StaffName";
            AStaff.StaffName = testData;
            Assert.AreEqual(AStaff.StaffName, testData);
        }
        public void StaffAddressOK()
        {
            // Create an instance class
            clsStaff AStaff = new clsStaff();
            // Create test data
            string testData = "StaffAddress";
            AStaff.StaffAddress = testData;
            Assert.AreEqual(AStaff.StaffAddress, testData);
        }
        public void StaffEmailOK()
        {
            // Create an instance class
            clsStaff AStaff = new clsStaff();
            // Create test data
            string testData = "StaffEmail";
            AStaff.StaffEmail = testData;
            Assert.AreEqual(AStaff.StaffEmail, testData);
        }
        public void StaffPhoneNumberOK()
        {
            // Create an instance class
            clsStaff AStaff = new clsStaff();
            // Create test data
            int testData = 1;
            AStaff.StaffPhoneNumber = testData;
            Assert.AreEqual(AStaff.StaffPhoneNumber, testData);
        }
        public void PositionIDOK()
        {
            // Create an instance class
            clsStaff AStaff = new clsStaff();
            // Create test data
            int testData = 1;
            AStaff.PositionId = testData;
            Assert.AreEqual(AStaff.PositionId, testData);
        }
    }
}