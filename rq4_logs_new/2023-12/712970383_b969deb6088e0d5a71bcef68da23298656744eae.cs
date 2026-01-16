using LeetCodeTraining.SubstringPermutation;

namespace LeetCodeTraining.Tests
{
    public class SubstringPermutationTests
    {
        [Theory]
        [MemberData(nameof(Data))]
        public void FindSubstring(string s, string[] words, IList<int> expectedResult)
        {
            //Act
            IList<int> result = SubstringPermutationSolution.FindSubstring(s, words);

            //Assert
            Assert.Equal(expectedResult, result);
        }

        public static IEnumerable<object[]> Data =>
            new List<object[]>
            {
                new object[]
                {
                    "barfoothefoobarman",
                    new string[] { "foo", "bar" },
                    new List<int>() { 0, 9 }
                },
                new object[]
                {
                    "wordgoodgoodgoodbestword",
                    new string[] { "word", "good", "best", "word" },
                    new List<int>() { }
                },
                new object[]
                {
                    "barfoofoobarthefoobarman",
                    new string[] { "bar", "foo", "the" },
                    new List<int>() { 6, 9, 12 }
                },
                new object[]
                {
                    "aaaaaaaaaa",
                    new string[] { "a", "a" },
                    new List<int>() { 0, 1, 2, 3, 4, 5, 6, 7, 8 }
                },
                new object[]
                {
                    "abbabbaaba",
                    new string[] { "ab", "ba" },
                    new List<int>() { 0, 3, 5 }
                },
                new object[]
                {
                    "aaacddcaa",
                    new string[] { "dc", "cd" },
                    new List<int>() { 3 }
                }
            };
    }
}