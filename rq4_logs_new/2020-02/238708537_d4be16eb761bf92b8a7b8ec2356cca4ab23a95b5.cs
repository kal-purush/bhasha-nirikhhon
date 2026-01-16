using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NameDayLibrary;
using Xunit;

namespace NameDayLibrary.Tests
{
    public class ArgumentValidationTests
    {

        [Theory]
        [InlineData("1.1.")]
        [InlineData("10.1.")]
        [InlineData("21.11.")]
        [InlineData("2.12.")]
        public void ValidateDate_ShouldReturnString(string x)
        {
            string actual = ArgumentValidation.ValidateDate(x);

            Assert.True(actual.Length > 0);
        }
    }
}