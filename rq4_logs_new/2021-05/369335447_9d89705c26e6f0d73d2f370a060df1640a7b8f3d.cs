using System;
using Acme.Common;
using Xunit;

namespace Acme.CommonTests
{
    public class StringHandlerTest
    {
        [Fact]
        public void InsertSpacesTestValid()
        {
            // Arrange
            var source = "MarioBrothers";
            var expected = "Mario Brothers";
            var handler = new StringHandler();

            // Act
            var actual = handler.InsertSpaces(source);

            // Assert
            Assert.False(expected == actual);
        }
    }
}