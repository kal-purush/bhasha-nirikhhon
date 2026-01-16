using BuddyDomain.Battle.Services;
using BuddyDomain.Battle.ValueObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TestDomain.Battle.Service
{
    [TestClass]
    public class TestAttackPowerBuilderInner
    {
        [TestMethod]
        public void Build()
        {
            var listenerMock = new Mock<IAttackPowerListener>();
            int powerResult = 0;
            listenerMock.Setup(l => l.ListenAttackPower(It.IsAny<int>()))
                .Callback<int>(p => powerResult = p);

            var builder = new AttackPowerBuilderInner();
            builder.ListenAttackValue(50);
            builder.ListenForceValue(100);
            var attackPower = builder.Build();
            attackPower.NotifyAttackPower(listenerMock.Object);
            Assert.AreEqual(5000, powerResult);
        }
    }
}