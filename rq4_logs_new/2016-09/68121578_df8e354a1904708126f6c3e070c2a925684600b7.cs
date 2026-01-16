using Microsoft.Bot.Connector;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MyMicrofostBot;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyMicrofostBot.Tests
{
    [TestClass()]
    public class MessagesControllerTests
    {
        [TestMethod()]
        public void PostTest()
        {
            var ctr = new MessagesController();
            var activity = new Activity(
                type: ActivityTypes.Message,
                text: "2d6"
                );

            // XXX ConnectorClient connector = new ConnectorClient(new Uri(activity.ServiceUrl)); を通す
            var result = ctr.Post(activity).Result;
        }
    }
}