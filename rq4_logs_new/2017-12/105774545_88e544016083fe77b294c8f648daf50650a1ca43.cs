using System;
using DNX.Helpers.Console;
using NUnit.Framework;
using Shouldly;

namespace Test.DNX.Helpers.Console
{
    [TestFixture]
    public class ConsoleColourChangerTests
    {
        [Ignore("Not currently working")]
        [Test]
        public void ConsoleColourChanger_can_set_and_restore_colour()
        {
            // Arrange
            var currentColour = System.Console.ForegroundColor;
            var requiredColour = ConsoleColor.DarkCyan;

            // Act
            using (var changer = new ConsoleColourChanger(requiredColour))
            {
                System.Console.ForegroundColor.ShouldBe(requiredColour);
            }

            // Assert
            System.Console.ForegroundColor.ShouldBe(currentColour);
        }
    }
}