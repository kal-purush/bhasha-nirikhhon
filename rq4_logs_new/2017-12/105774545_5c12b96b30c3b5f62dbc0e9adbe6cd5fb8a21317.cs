using System;
using DNX.Helpers.Console.Exceptions;
using NUnit.Framework;
using Shouldly;

namespace Test.DNX.Helpers.Console.Exceptions
{
    [TestFixture]
    public class ReturnCodeExceptionTests
    {
        [Test]
        public void Constructor_with_return_code_can_retain_values()
        {
            // Arrange
            const int returnCode = 42;

            // Act
            var ex = new ReturnCodeException(returnCode);

            // Assert
            ex.ShouldNotBeNull();
            ex.ReturnCode.ShouldBe(returnCode);
        }

        [Test]
        public void Constructor_with_return_code_and_message_can_retain_values()
        {
            // Arrange
            const int returnCode = 42;
            const string message = "Something went wrong";

            // Act
            var ex = new ReturnCodeException(returnCode, message);

            // Assert
            ex.ShouldNotBeNull();
            ex.ReturnCode.ShouldBe(returnCode);
            ex.Message.ShouldBe(message);
        }

        [Test]
        public void Constructor_with_return_code_and_message_and_inner_exception_can_retain_values()
        {
            // Arrange
            const int returnCode = 42;
            const string message = "Something went wrong";
            var innerException = new Exception();

            // Act
            var ex = new ReturnCodeException(returnCode, message, innerException);

            // Assert
            ex.ShouldNotBeNull();
            ex.ReturnCode.ShouldBe(returnCode);
            ex.Message.ShouldBe(message);
            ex.InnerException.ShouldBe(innerException);
        }
    }
}