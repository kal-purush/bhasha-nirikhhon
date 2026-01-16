using Xunit;

namespace StringUtilities.Tests
{
    public class StringTruncateTests
    {
        [Fact]
        public void Truncate_CanHandleEmptyStrings()
        {
            var value = "";

            var result = value.Truncate(10);

            Assert.Equal(value, result);
        }

        [Fact]
        public void Truncate_CanHandleNull()
        {
            string value = null;

            var result = value.Truncate(10);

            Assert.Equal(value, result);
        }

        [Fact]
        public void Truncate_ToLongerValue_CanHandleStringsShorterThan3Characters()
        {
            var value = "Ab";

            var result = value.Truncate(10);

            Assert.Equal(value, result);
        }

        [Fact]
        public void Truncate_ToExactLength_CanHandleStringsShorterThan3Characters()
        {
            var value = "Ab";

            var result = value.Truncate(2);

            Assert.Equal(value, result);
        }

        [Fact]
        public void Truncate_ToShorterValue_CanHandleStringsShorterThan3Characters()
        {
            var value = "Ab";

            var result = value.Truncate(1);

            Assert.Equal("A", result);
        }

        [Fact]
        public void Truncate_ToLongerValue_CanHandleStringsExactly3Characters()
        {
            var value = "Abc";

            var result = value.Truncate(10);

            Assert.Equal(value, result);
        }

        [Fact]
        public void Truncate_ToShorterValue_CanHandleStringsExactly3Characters()
        {
            var value = "Abc";

            var result = value.Truncate(2);

            Assert.Equal("Ab", result);
        }

        [Fact]
        public void Truncate_ToExactLength_CanHandleStringsExactly3Characters()
        {
            var value = "Abc";

            var result = value.Truncate(3);

            Assert.Equal(value, result);
        }

        [Fact]
        public void Truncate_ToLongerValue_CanHandleStringsLongerThan3Characters()
        {
            var value = "Abcdefghijklmnopqrstuvwxyz";

            var result = value.Truncate(30);

            Assert.Equal(value, result);
        }

        [Fact]
        public void Truncate_ToShorterThan3_CanHandleStringsLongerThan3Characters()
        {
            var value = "Abcdefghijklmnopqrstuvwxyz";

            var result = value.Truncate(2);

            Assert.Equal("Ab", result);
        }

        [Fact]
        public void Truncate_ToExactly3_CanHandleStringsLongerThan3Characters()
        {
            var value = "Abcdefghijklmnopqrstuvwxyz";

            var result = value.Truncate(3);

            Assert.Equal("Abc", result);
        }

        [Fact]
        public void Truncate_ToLongerThan3_CanHandleStringsLongerThan3Characters()
        {
            var value = "Abcdefghijklmnopqrstuvwxyz";

            var result = value.Truncate(4);

            Assert.Equal("A...", result);
        }

        [Fact]
        public void Truncate_To1LessThanLength_CanHandleStringsLongerThan3Characters()
        {
            var value = "Abcdefghijklmnopqrstuvwxyz";

            var result = value.Truncate(25);

            Assert.Equal("Abcdefghijklmnopqrstuv...", result);
        }
    }
}