using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SDMBLL.Services;
using SDMEntity.BE;

namespace UnitTestProject1
{

    [TestClass]
    public class UnitTestProjectRequest
    {
        [TestMethod]
        public void TestCreateProjectRequest()
        {
            var serv = new ProjectRequestService();
            var pr = new ProjectRequest()
            {
                Priority = 1,
                IsAccepted = true
        
            };

            var newPr = serv.Create(pr);

            Assert.AreEqual(pr.Priority, 1);
        }

        [TestMethod]
        public void TestGetAllProjectRequest()
        {
            var serv = new ProjectRequestService();
            var pr = new ProjectRequest()
            {
                Priority = 1,
                IsAccepted = true
            }; var pr2 = new ProjectRequest()
            {
                Priority = 1,
                IsAccepted = true
            };

            var newPr = serv.Create(pr);
            var newPr2 = serv.Create(pr2);

            Assert.AreEqual(serv.GetAll().Count, 2);
        }

    }
}