using System;
using System.IO;
using NUnit.Framework;
using TicTacToe;

namespace TicTacToeTest
{
    [TestFixture]
    public class PromptTest
    {
        [Test]
        public void Get_Name()
        {
            StringReader reader = new StringReader("Kirby\n");
            Console.SetIn(reader);
            string name = Prompt.GetPlayerName();
            Assert.AreEqual("Kirby", name);
        }

        [Test]
        public void Get_Piece()
        {
            StringReader reader = new StringReader("X\n");
            Console.SetIn(reader);
            string name = Prompt.GetPlayerPiece();
            Assert.AreEqual("X", name);
        }


        [Test]
        public void Get_Move()
        {
            StringReader reader = new StringReader("4\n");
            Console.SetIn(reader);
            string move = Prompt.GetPlayerMove("Bob");
            Assert.AreEqual("4", move);
        }

    }
}