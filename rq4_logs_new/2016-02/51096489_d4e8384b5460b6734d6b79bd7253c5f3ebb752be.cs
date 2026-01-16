using System;
using TicTacToe;
using NUnit.Framework;
using System.IO;

namespace UnitTestProject1
{
    [TestFixture]
    public class MessageFactoryTest
    {
        [Test]
        public void GameAsksPlayerForMove()
        {
            string name = "Robert";
            string movePrompt = MessageFactory.AskPlayerForMove(name);
            Assert.AreEqual(movePrompt, "Where would you like to move Robert");
        }

        [Test]
        public void GameAsksPlayerForTheirName()
        {
            string namePrompt = MessageFactory.AskPlayerForName();
            Assert.AreEqual("What is your name?", namePrompt);
        }
    }
}