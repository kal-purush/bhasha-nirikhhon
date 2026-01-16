using BuddyDomain.Battle.Services;
using BuddyDomain.Battle.ValueObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TestDomain.Battle.Service
{
    [TestClass]
    public class TestDamageBuilder
    {
        [TestMethod]
        public void Build()
        {
            var damageListenerMock = new Mock<IDamageListener>();
            int damageHolder = 0;
            damageListenerMock.Setup(
                    l => l.ListenDamage(It.IsAny<int>()))
                .Callback<int>(d => damageHolder = d);
            var damageBuilder = new DamageBuilder();
            damageBuilder.ListenAttackPower(100);
            damageBuilder.ListenDefenseRate(0.7f);
            var damage = damageBuilder.Build();
            damage.NotifyDamage(damageListenerMock.Object);
            Assert.AreEqual(70, damageHolder);
        }
    }
}