using System;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Utils.Events;

namespace UtilsUnitTests.Events
{
    [TestClass]
    public class EventsTests
    {
        [TestMethod]
        public void TestDynamicEventArgs()
        {

            try
            {
                var @event = new DynamicEventArgs();
                @event.Args.test = "New test Property";
                @event.Args.func = new Func<String, String>(s => s.ToUpper());

                Assert.AreEqual("New test Property", @event.Args.test, false);

                Assert.AreEqual("TOTO", @event.Args.func("toto"), false);

            }
            catch (Exception e)
            {
                Assert.Fail("Exception : {0}", e);
            }
        }
    }
}