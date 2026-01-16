using BuddyDomain.Battle.Services;
using BuddyDomain.Battle.ValueObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TestDomain.Battle.Service
{
    [TestClass]
    public class TestDefenseRateBuilder
    {
        [TestMethod]
        public void Build()
        {
            var listenerMock = new Mock<IDefenseRateListener>();
            float rateResult = 0;
            listenerMock.Setup(l => l.ListenDefenseRate(It.IsAny<float>()))
                .Callback<float>(f => rateResult = f);
            var defenseRateBuilder = new DefenseRateBuilder();
            var defenseRate = defenseRateBuilder.Build();
            defenseRate.NotifyDefenseRate(listenerMock.Object);
            Assert.AreEqual(1, rateResult);
        }
    }
}