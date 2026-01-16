using System;
using Xunit;
using MultibracketValidation;

namespace MultibracketValidTests
{
    public class BracketValidation
    {
        [Theory]
        [InlineData ("({}[])")]
        [InlineData("Kona is Super cute!")]
        [InlineData("{}[]()")]
        public void Passes(string validate)
        {
            Assert.True(Program.MultiBracketValidation(validate));
        }

        [Theory]
        [InlineData ("({}[)]")]
        [InlineData("}")]
        [InlineData("[")]
        [InlineData("[)")]

        public void Fails(string validate)
        {
            Assert.False(Program.MultiBracketValidation(validate));
        }
    }
}