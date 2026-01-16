using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nexus.Link.Libraries.Core.Application;
using Nexus.Link.Libraries.Core.Json;
using UT = Microsoft.VisualStudio.TestTools.UnitTesting;
// ReSharper disable ClassNeverInstantiated.Local
// ReSharper disable UnusedAutoPropertyAccessor.Local

namespace Nexus.Link.Libraries.Core.Tests.Json
{
    [TestClass]
    public class TestJsonHelper
    {

        [TestInitialize]
        public void Initialize()
        {
            FulcrumApplicationHelper.UnitTestSetup(typeof(TestJsonHelper).FullName);
        }

        [TestMethod]
        public void TryDeserializeNull()
        {
            var success = JsonHelper.TryDeserializeObject<SimpleObject>(null, out var simpleObject);
            UT.Assert.IsFalse(success);
            UT.Assert.IsNull(simpleObject);
            simpleObject = JsonHelper.SafeDeserializeObject<SimpleObject>(null);
            UT.Assert.IsNull(simpleObject);
        }

        [TestMethod]
        public void TryDeserializeEmptyString()
        {
            var success = JsonHelper.TryDeserializeObject<SimpleObject>("", out var simpleObject);
            UT.Assert.IsTrue(success);
            UT.Assert.IsNull(simpleObject);
            simpleObject = JsonHelper.SafeDeserializeObject<SimpleObject>("");
            UT.Assert.IsNull(simpleObject);
        }

        [TestMethod]
        public void TryDeserializeXml()
        {
            const string value = "<html/>";
            var success = JsonHelper.TryDeserializeObject<SimpleObject>(value, out var simpleObject);
            UT.Assert.IsFalse(success);
            UT.Assert.IsNull(simpleObject);
            simpleObject = JsonHelper.SafeDeserializeObject<SimpleObject>(value);
            UT.Assert.IsNull(simpleObject);
        }

        [TestMethod]
        public void TryDeserializeEmptyObject()
        {
            const string value = "{}";
            var success = JsonHelper.TryDeserializeObject<SimpleObject>(value, out var simpleObject);
            UT.Assert.IsTrue(success);
            UT.Assert.IsNotNull(simpleObject);
            UT.Assert.IsNull(simpleObject.Name);
            simpleObject = JsonHelper.SafeDeserializeObject<SimpleObject>(value);
            UT.Assert.IsNotNull(simpleObject);
            UT.Assert.IsNull(simpleObject.Name);
        }

        [TestMethod]
        public void TryDeserializeEmptyStringMandatoryField()
        {
            var success = JsonHelper.TryDeserializeObject<HasMandatoryField>("", out var hasMandatoryField);
            UT.Assert.IsTrue(success);
            UT.Assert.IsNull(hasMandatoryField);
            UT.Assert.IsNull(hasMandatoryField);
            hasMandatoryField = JsonHelper.SafeDeserializeObject<HasMandatoryField>("");
            UT.Assert.IsNull(hasMandatoryField);
        }

        [TestMethod]
        public void TryDeserializeEmptyObjectMandatoryField()
        {
            const string value = "{}";
            var success = JsonHelper.TryDeserializeObject<HasMandatoryField>(value, out var hasMandatoryField);
            UT.Assert.IsTrue(success);
            UT.Assert.IsNotNull(hasMandatoryField);
            UT.Assert.AreEqual(default(int), hasMandatoryField.Value);
            hasMandatoryField = JsonHelper.SafeDeserializeObject<HasMandatoryField>(value);
            UT.Assert.IsNotNull(hasMandatoryField);
            UT.Assert.AreEqual(default(int), hasMandatoryField.Value);
        }

        [TestMethod]
        public void TryDeserializeMandatoryField()
        {
            const string value = "{\"Value\": 3}";
            var success = JsonHelper.TryDeserializeObject<HasMandatoryField>(value, out var hasMandatoryField);
            UT.Assert.IsTrue(success);
            UT.Assert.IsNotNull(hasMandatoryField);
            UT.Assert.AreEqual(3, hasMandatoryField.Value);
            hasMandatoryField = JsonHelper.SafeDeserializeObject<HasMandatoryField>(value);
            UT.Assert.IsNotNull(hasMandatoryField);
            UT.Assert.AreEqual(3, hasMandatoryField.Value);
        }

        private class SimpleObject
        {
            public string Name { get; set; }
        }

        private class HasMandatoryField
        {
            public int Value { get; set; }
        }
    }
}