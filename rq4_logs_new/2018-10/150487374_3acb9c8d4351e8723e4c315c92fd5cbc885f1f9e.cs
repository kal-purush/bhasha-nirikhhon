using System;
using Xunit;

namespace wimm.Missionary.UnitTests
{
    public class FromStringConversionTest
    {
        [Fact]
        public void Supported_TypeIsNotSupported_ReturnsFalse()
        {
            Assert.False(FromStringConversion<object>.Supported());
        }

        [Fact]
        public void Supported_TypeHasStringCtor_ReturnsTrue()
        {
            Assert.True(FromStringConversion<Uri>.Supported());
        }

        [Fact]
        public void Supported_TypeHasParseMethod_ReturnsTrue()
        {
            Assert.True(FromStringConversion<int>.Supported());
        }

        [Fact]
        public void Construct_TypeIsNotSupported_Throws()
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
            {
                var _ = new FromStringConversion<object>();
            });
        }

        [Fact]
        public void Construct_TypeIsSupportedByConstructor_Constructs()
        {
            var _ = new FromStringConversion<Uri>();
        }

        [Fact]
        public void Construct_TypeIsSupportedByParse_Constructs()
        {
            var _ = new FromStringConversion<int>();
        }

        [Fact]
        public void Convert_CtorNullFrom_Throws()
        {
            var underTest = new FromStringConversion<Uri>();

            var ex = Assert.Throws<ArgumentNullException>(() =>
            {
                var _ = underTest.Convert(null);
            });
        }

        [Fact]
        public void Convert_CtorInvalidArg_Throws()
        {
            var underTest = new FromStringConversion<Uri>();

            var ex = Assert.Throws<InvalidCastException>(() =>
            {
                var _ = underTest.Convert("+++ not a uri +++");
            });
        }

        [Fact]
        public void Convert_CtorValidArg_ConvertsValue()
        {
            var from = "https://www.company.com";
            var expected = new Uri(from);
            var underTest = new FromStringConversion<Uri>();

            var actual = underTest.Convert(from);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void Convert_ParseNullFrom_Throws()
        {
            var underTest = new FromStringConversion<Uri>();

            var ex = Assert.Throws<ArgumentNullException>(() =>
            {
                var _ = underTest.Convert(null);
            });
        }

        [Fact]
        public void Convert_ParseInvalidArg_Throws()
        {
            var underTest = new FromStringConversion<Uri>();

            var ex = Assert.Throws<InvalidCastException>(() =>
            {
                var _ = underTest.Convert("+++ not an int +++");
            });
        }

        [Fact]
        public void Convert_ParseValidArg_ConvertsValue()
        {
            var from = "42";
            var expected = int.Parse(from);
            var underTest = new FromStringConversion<int>();

            var actual = underTest.Convert(from);

            Assert.Equal(expected, actual);
        }
    }
}