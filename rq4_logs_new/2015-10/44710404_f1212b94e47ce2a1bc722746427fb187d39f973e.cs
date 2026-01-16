using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Brello.Models;

namespace Brello.Tests.Models
{
    [TestClass]
    public class CardTests
    {
        [TestMethod]
        public void CardEnsureICanCreateInstance()
        {
            Card c = new Card();
            Assert.IsNotNull(c);
        }

        [TestMethod]
        public void CardEnsurePropertiesWork()
        {
            Color color = new Color { Name = "Blue", Value = "#0000ff" };
            // Object Initializer syntax
            Card c = new Card { Title = "My Card", Description = "A description of my card", BorderColor = color};
            // Otherwise you'd have to
            Assert.AreEqual("My Card", c.Title);
            Assert.AreEqual("A description of my card", c.Description);
            Assert.AreEqual("Blue", c.BorderColor.Name);
        }
    }

}