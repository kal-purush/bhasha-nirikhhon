using FindMatches;
using KAryTree;
using System;
using Xunit;

namespace FindMatchesTesting
{
    public class FindMatchesTesting
    {
        /// <summary>
        /// Tests four test cases of FindMatches using data taken from FindMatchesTestingData
        /// </summary>
        /// <param name="tree">The tree used for testing</param>
        /// <param name="searchChar">The character to search for in this test</param>
        /// <param name="expectedMatchCount">The expected number of matches for searchChar</param>
        [Theory]
        [ClassData(typeof(FindMatchesTestingData))]
        public void FindMatchesTest(KAryTree<char> tree, char searchChar, int expectedMatchCount)
        {
            // Act
            int actualMatchCount = Program.FindMatches(tree, searchChar).Count;

            // Assert
            Assert.Equal(expectedMatchCount, actualMatchCount);
        }
    }
}