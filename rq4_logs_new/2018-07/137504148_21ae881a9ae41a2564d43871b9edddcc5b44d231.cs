using WordCount.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests.EnumerableStringExtensions
{
    public class FirstOfMatchingRegexTests
    {
        [NamedFact]
        public void FirstOfMatchingRegexTests_Two_Values_Matching_Only_one_Expect_one()
        {
            List<string> list = new List<string>()
            {
                "abc",
                "def"
            };

            string actual = list.FirstOfMatchingRegex(pattern: "abc");

            Assert.Equal(
                expected: "abc",
                actual: actual);
        }
    }
}