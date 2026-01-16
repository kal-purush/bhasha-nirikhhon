using System;
using System.Web.Http.Results;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nikita.API.Controllers;
using Nikita.API.Controllers.Commands;
using Nikita.API.Models;
using Nikita.API.Models.Interfaces;
using Unity;

namespace Nikita.API.UnitTest
{
    [TestClass]
    public class PeopleControllerTest
    {
        private PeopleCommandFactory commandFactory;
        private PeopleController peopleController;
        [TestInitialize]
        public void InitTest()
        {
            var container = Nikita.API. UnityConfig.GetConfiguredContainer();
              commandFactory  = container.Resolve<PeopleCommandFactory>();

            peopleController = new Nikita.API.Controllers.PeopleController(commandFactory);
        }
        

        [TestMethod]
        public void GetPeopleSuccess_NoItems()
        {
            RunPeopleSuccess_NoItems_Test("gender",0);
            RunPeopleSuccess_NoItems_Test("name",0);
            RunPeopleSuccess_NoItems_Test("birthdate",0);   
        }

        public void RunPeopleSuccess_NoItems_Test(string sort, int expectedCount)
        {
             var response = peopleController.GetPeople(sort);
            var contentResult = response as OkNegotiatedContentResult<IPersonList>;
            Assert.IsNotNull(contentResult);
            Assert.IsNotNull(contentResult.Content);
            Assert.IsInstanceOfType(contentResult.Content, typeof(IPersonList));
            var concreteObject = contentResult.Content as PersonList;

            Assert.AreEqual(expectedCount, concreteObject.Count);
        }


        [TestMethod]
        public void GetPeopleFail_InvalidSort()
        {
          
            var response = peopleController.GetPeople("XXX");
            var contentResult = response as BadRequestErrorMessageResult;    
            Assert.IsNotNull(contentResult);
            Assert.AreEqual("Invalid sort request", contentResult.Message); 
        }

        [TestMethod]
        public void PostPeople_csv_Success_Test()
        {
           var response =  peopleController.PutPerson(new PersonDataPut() {Data = "Rolkov,DDichael,F,green,04/30/1986" });
            var contentResult = response as OkResult;
            Assert.IsNotNull(contentResult);
            
        }
        [TestMethod]
        public void PostPeople_pipe_Success_Test()
        {
            var response = peopleController.PutPerson(new PersonDataPut() { Data = "Rolkov|DDichael|F|green|04/30/1986" });
            var contentResult = response as OkResult;
            Assert.IsNotNull(contentResult);

        }
        [TestMethod]
        public void PostPeople_space_Success_Test()
        {
            var response = peopleController.PutPerson(new PersonDataPut() { Data = "Rolkov DDichael F green 04/30/1986" });
            var contentResult = response as OkResult;
            Assert.IsNotNull(contentResult);

        }

        [TestMethod]
        public void GetPeopleSuccess_HasItems()
        {                    
            RunPeopleSuccess_NoItems_Test("gender", 3);
            RunPeopleSuccess_NoItems_Test("name",3);
            RunPeopleSuccess_NoItems_Test("birthdate", 3);
        }

        [TestMethod]
        public void PostPeople_space_Fail_Test()
        {
            var response = peopleController.PutPerson(new PersonDataPut() { Data = "RolkovDDichaelFgreen04/30/1986" });
            var contentResult = response as BadRequestErrorMessageResult;

            Assert.IsNotNull(contentResult);
            Assert.AreEqual("Invalid Person raw data", contentResult.Message);

        }
    }
}