using Microsoft.VisualStudio.TestTools.UnitTesting;
using IncrementService.Controllers;
using IncrementService.Models;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Web;
using System.Web.Http;
using System.Net.Http;

namespace IncrementService.Tests.Controllers
{
    [TestClass]
    public class IncrementControllerTest
    {
        [TestMethod]
        public void PostNewKeyTest()
        {
            // Arrange
            string keyName = "addkeytest";
            var input = new Increment() { Key = keyName, InitialStart = 0, IsDeletable = true };
            IncrementController controller = CreateControllerInstance();

            // delete the key if it exists and recreate...
            controller.Delete(keyName);
            controller.Post(input);

            long result = GetLongfromHttpResponse(controller.Get(keyName));
            Assert.AreEqual(input.InitialStart + 1, result);    // we just did a GET, so the returned value should be InitialStart + 1

            controller.Delete(keyName);
            List<string> keyList = GetStringListFromHttpResponse(controller.Get());
            CollectionAssert.DoesNotContain(keyList, keyName);  // verify keyName was removed
        }

        [TestMethod]
        public void GetAllKeysTest()
        {
            string keyName = "getallkeystest";
            var input = new Increment() { Key = keyName, InitialStart = 0, IsDeletable = true };
            IncrementController controller = CreateControllerInstance();

            // delete the key if it exists and recreate...
            controller.Delete(keyName);
            controller.Post(input);

            List<string> keyList = GetStringListFromHttpResponse(controller.Get());
            CollectionAssert.Contains(keyList, keyName);        // verify keyName was added

            controller.Delete(keyName); // test cleanup
        }

        [TestMethod]
        public void GetASpecificKeyTest()
        {
            string keyName = "specifickeytest";
            var input = new Increment() { Key = keyName, InitialStart = 2, IsDeletable = true };
            IncrementController controller = CreateControllerInstance();
            controller.Delete(keyName); // delete the key if it exists and recreate...
            controller.Post(input);

            long result = GetLongfromHttpResponse(controller.Get(keyName));
            Assert.AreEqual(input.InitialStart + 1, result);    // we just did a GET, so the returned value should be InitialStart + 1

            controller.Delete(keyName); // test cleanup
        }

        [TestMethod]
        public void DeleteAKeyTest()
        {
            string keyName = "deletekeytest";
            var input = new Increment() { Key = keyName, InitialStart = -5, IsDeletable = true };
            IncrementController controller = CreateControllerInstance();
            controller.Post(input);

            List<string> keys = GetStringListFromHttpResponse(controller.Get());
            CollectionAssert.Contains(keys, keyName);       // verify the key was created

            controller.Delete(keyName);

            keys = GetStringListFromHttpResponse(controller.Get());
            CollectionAssert.DoesNotContain(keys, keyName); // verify keyName was removed
        }

        [TestMethod]
        public void DeleteAKeyThatIsNotDeletableTest()
        {
            string keyName = "deleteANotDeletablekeytest";
            var input = new Increment() { Key = keyName, InitialStart = 1, IsDeletable = false };
            IncrementController controller = CreateControllerInstance();
            controller.Post(input);

            List<string> keys = GetStringListFromHttpResponse(controller.Get());
            CollectionAssert.Contains(keys, keyName);       // verify the key was created

            controller.Delete(keyName);

            keys = GetStringListFromHttpResponse(controller.Get());
            CollectionAssert.Contains(keys, keyName); // verify keyName was not removed
        }

        private static long GetLongfromHttpResponse(IHttpActionResult response)
        {
            HttpResponseMessage result = response.ExecuteAsync(new System.Threading.CancellationToken()).Result;
            return result.Content.ReadAsAsync<long>().Result;
        }

        private static List<string> GetStringListFromHttpResponse(IHttpActionResult response)
        {
            HttpResponseMessage result = response.ExecuteAsync(new System.Threading.CancellationToken()).Result;
            return result.Content.ReadAsAsync<List<string>>().Result;
        }

        private static IncrementController CreateControllerInstance()
        {
            IncrementController controller = new IncrementController();
            controller.Request = new HttpRequestMessage();
            controller.Configuration = new HttpConfiguration();
            return controller;
        }
    }
}