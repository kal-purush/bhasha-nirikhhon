using System.Threading;
using NUnit.Framework;

namespace NLog.Targets.SNS.Tests
{
    [TestFixture]
    public class AmazonSimpleNotificationServiceTargetTests
    {
        [Test]
        public void Test()
        {
            var logger = LogManager.GetCurrentClassLogger();
            logger.Info("hello world");
        }
    }
}