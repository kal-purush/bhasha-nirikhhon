using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NSubstitute;
using NUnit.Framework;
using Microwave.Classes.Boundary;
using Microwave.Classes.Interfaces;

namespace Microwave.Test.Unit
{
    [TestFixture]
    public class BuzzerTest
    {
        private Buzzer uut;
        private IOutput output;

            [SetUp]
        public void Setup()
        {
            output = Substitute.For<IOutput>();

            uut = new Buzzer(output);
        }

        [Test]
        public void TurnOn_NoSubscribers_NoThrow()
        {
            // We don't need an assert, as an exception would fail the test case
            uut.Press();
        }

        [Test]
        public void Press_1subscriber_IsNotified()
        {
            bool notified = false;

            uut.Pressed += (sender, args) => notified = true;
            uut.Press();
            Assert.That(notified, Is.EqualTo(true));
        }

    }
}