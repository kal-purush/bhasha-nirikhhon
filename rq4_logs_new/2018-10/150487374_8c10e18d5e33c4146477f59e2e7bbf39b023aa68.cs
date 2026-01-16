using System;
using Xunit;

namespace wimm.Missionary.UnitTests
{
    public class FromStringConverterTest
    {
        [Fact]
        public void To_TypeIsNotSupported_Throws()
        {
            var underTest = new FromStringConverter();

            var ex = Assert.Throws<InvalidOperationException>(() => underTest.To<object>("object"));
        }

        [Fact]
        public void To_NullFrom_Throws()
        {
            var underTest = new FromStringConverter();

            var ex = Assert.Throws<ArgumentNullException>(() => underTest.To<int>(null));

            Assert.Equal("from", ex.ParamName);
        }

        [Fact]
        public void To_ConversionThrows_ExceptionIsWrapped()
        {
            var underTest = new FromStringConverter();

            var ex = Assert.Throws<InvalidCastException>(() => underTest.To<int>("forty-two"));
        }

        [Fact]
        public void To_ConversionWorks_ReturnsValueFromConversion()
        {
            var from = "42";
            var expected = int.Parse(from);
            var underTest = new FromStringConverter();
            var actual = underTest.To<int>(from);

            Assert.Equal(expected, actual);
        }
    }
}