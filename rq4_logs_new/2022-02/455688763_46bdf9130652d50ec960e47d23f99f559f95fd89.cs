using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Question2.UnitTests
{
    public class RemoveValuesRepeatedMoreThanTwice_Tests
    {
        [Fact]
        public void ThrowsNullReferenceException_IfLinkedListIsNull()
        {
            // Arrange
            LinkedList<string> nullLinkedList = null;

            // Act
            Action action = () => nullLinkedList.RemoveValuesRepeatedMoreThanTwice();

            // Assert
            Assert.Throws<NullReferenceException>(action);
        }

        [Fact]
        public void ThrowsArgumentException_IfLinkedListHasNoItems()
        {
            // Arrange
            LinkedList<string> nullLinkedList = new LinkedList<string>();

            // Act
            Action action = () => nullLinkedList.RemoveValuesRepeatedMoreThanTwice();

            // Assert
            Assert.Throws<ArgumentException>(action);
        }

        [Theory()]
        [MemberData(nameof(TestData))]
        public void RemovesValuesRepeatedMoreThanTwice(List<string> test, List<string> expected)
        {
            // Arrange
            LinkedList<string> lettersLinkedList = new LinkedList<string>(test);

            // Act
            var result = lettersLinkedList.RemoveValuesRepeatedMoreThanTwice();
            string[] lettersArray = new string[result.Count];
            result.CopyTo(lettersArray, 0);

            // Assert
            Assert.Equal(expected, lettersArray.ToList());
        }

        public static IEnumerable<object[]> TestData => new List<object[]> 
        {
            new object[] { new List<string> { "A", "B", "C"  }, new List<string> { "A", "B", "C" } },
            new object[] { new List<string> { "A", "A", "B", "B", "C", "C"  }, new List<string> { "A", "A", "B", "B", "C", "C" } },
            new object[] { new List<string> { "A", "A", "A", "B", "B", "B", "C", "C", "C"  }, new List<string> { "A", "A", "B", "B", "C", "C" } },
            new object[] { new List<string> { "A", "A", "A", "A", "B", "B", "B", "B", "C", "C", "C", "C"  }, new List<string> { "A", "A", "B", "B", "C", "C" } },
            new object[] { new List<string> { "E", "B", "E", "E", "B", "A", "B" }, new List<string> { "E", "B", "E", "B", "A" } },
        };
    }
}