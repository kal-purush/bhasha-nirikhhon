using BuddyDomain.Battle.Services;
using BuddyDomain.Battle.ValueObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TestDomain.Battle.Service
{
    [TestClass]
    public class TestAttackPowerBuilder
    {
        [TestMethod]
        public void Build()
        {
            var innerBuilder = new Mock<IAttackPowerBuilderInner>();
            var attackPowerMock = new AttackPower(5000);
            innerBuilder.Setup(i => i.Build()).Returns(attackPowerMock);

            var builder = new AttackPowerBuilder(innerBuilder.Object);
            var attackPower = builder.Build();

            var mockListener = new Mock<IAttackPowerListener>();
            mockListener.Setup(l => l.ListenAttackPower(It.IsAny<int>()))
                .Callback<int>(power => Assert.AreEqual(5000, power));
            attackPower.NotifyAttackPower(mockListener.Object);
        }
    }
}