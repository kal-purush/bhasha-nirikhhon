using System;
using CardGame.Model.Cards.Interfaces;
using CardGame.Model.Engine;
using Moq;
using NUnit.Framework;

namespace CardGameTests.Model.Engine
{
    [TestFixture]
    public class GameEventManagerTests
    {
        private Mock<ICard> _card0;
        private Mock<ICard> _card1;

        [SetUp]
        public void Setup()
        {
            //Initialize GameEventManager
            GameEventManager.Initialize();

            _card0 = new Mock<ICard>(MockBehavior.Strict);
            _card0.Setup(c => c.Id).Returns(1);
            _card0.Setup(c => c.PlayOrder).Returns(1);

            _card1 = new Mock<ICard>(MockBehavior.Strict);
            _card1.Setup(c => c.Id).Returns(2);
            _card1.Setup(c => c.PlayOrder).Returns(2);
        }
        
        private void TestTrigger(out bool value)
        {
            value = true;
        }

        private void UnregisterTriggers()
        {
            GameEventManager.UnregisterForEvents(_card0.Object);
            GameEventManager.UnregisterForEvents(_card1.Object);
        }

        [Test]
        public void OnStartTurn_CardHasNoTriggers_NothingHappens()
        {
            //Assign

            //Act
            GameEventManager.OnTurnStart();

            //Assert
            //---should not throw an exception
        }

        [Test]
        public void OnStartTurn_CardHasOnStartTurnTrigger_TriggerFires()
        {
            //Assign
            var value = false;
            GameEventManager.RegisterForEventTurnStart(_card0.Object, () => TestTrigger(out value));

            //Act
            GameEventManager.OnTurnStart();

            //Assert
            Assert.That(value, Is.True);

            UnregisterTriggers();
        }

        [Test]
        public void OnStartTurn_CardHasBothOnStartTurnTriggerAndOnStartEndTrigger_OnlyValue1IsTrue()
        {
            //Assign
            var value1 = false;
            var value2 = false;
            GameEventManager.RegisterForEventTurnStart(_card0.Object, () => TestTrigger(out value1));
            GameEventManager.RegisterForEventTurnEnd(_card0.Object, () => TestTrigger(out value2));

            //Act
            GameEventManager.OnTurnStart();

            //Assert
            Assert.That(value1, Is.True);
            Assert.That(value2, Is.False);

            UnregisterTriggers();
        }

    }
}