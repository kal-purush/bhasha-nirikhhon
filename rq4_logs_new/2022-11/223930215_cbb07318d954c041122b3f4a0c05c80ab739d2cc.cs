using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using FluentAssertions;
using NUnit.Framework;
using Peazy.Extensions;

namespace Peazy.Tests.Extensions.EnumerableExtensions
{
    public class DistinctByTest : BaseTest<DistinctByTest>
    {
        private string[] _source;
        private string _value;
        private Func<string, string> _selector;

        public DistinctByTest()
        {
            _test = this;
        }

        [SetUp]
        public void Setup()
        {
            _source = default;
            _value = default;
            _selector = default;
        }

        #if !NET6_0_OR_GREATER
        [Test]
        public void TwoEqualElementsShouldProduceSingleValue()
        {
            var data = new [] { "test", "test" };
            var distinct = data.DistinctBy(d => d);
            distinct.Should().HaveCount(1);
        }

        [Test]
        public void TwoSimilarElementsShouldProduceOneValue()
        {
            var data = new [] { "TeSt", "tEsT" };
            var comparer = new SloppyStringEqualityComparer();
            var distinct = data.DistinctBy(d => d, comparer);
            distinct.Should().HaveCount(1);
        }
        #endif

        private class SloppyStringEqualityComparer : IEqualityComparer<string>
        {
            public bool Equals([AllowNull] string x, [AllowNull] string y)
            {
                var anyNull = (x ?? y) == null;
                if (anyNull || (x == null && y != null))
                {
                    return false;
                }

                var sloppyX = x.Trim().ToLowerInvariant();
                var sloppyY = y.Trim().ToLowerInvariant();
                return sloppyX.Equals(sloppyY);
            }

            public int GetHashCode([DisallowNull] string obj)
            {
                var sloppy = obj.Trim().ToLowerInvariant();
                return sloppy.GetHashCode();
            }
        }
    }
}