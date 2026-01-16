using System;
using System.Collections.Generic;
using System.Linq;
using WordCount.Extensions;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests.ListExtensionsTests
{
    public class ToEmptyIfNullListTests
    {
        [NamedFact]
        public void ToEmptyIfNullListTests_Enumerable_is_null_NullIfEmpty_is_true_expect_empty_list()
        {
            string[] array = null;

            List<string> actual = array.ToEmptyIfNullList();

            Assert.NotNull(@object: actual);
            Assert.Empty(collection: actual);
        }
    }
}