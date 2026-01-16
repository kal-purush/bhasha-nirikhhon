using System;
using NUnit.Framework;

namespace Mercury.BinPack.Tests
{
    public sealed class SizeCases : SpecificationByMethod
    {
        private ISpecification Spec { get { throw new InvalidOperationException(); } set { Spec(value); } }

        protected override void Cases()
        {
            Spec = "When create size #w,#h"
                .ArrangeNull()
                .With(new { w = 1, h = 1, expectedVolume = 1, expectedToString = "1x1" })
                .With(new { w = 2, h = 1, expectedVolume = 2, expectedToString = "2x1" })
                .With(new { w = 2, h = 2, expectedVolume = 4, expectedToString = "2x2" })
                .With(new { w = 5, h = 4, expectedVolume = 20, expectedToString = "5x4" })
                .Act((_, d) => new Size(d.w, d.h))
                .Assert("Volume is #expectedVolume", (s, d) => Assert.AreEqual(d.expectedVolume, s.Volume))
            .Assert("ToString is #expectedToString", (s, d) => Assert.AreEqual(d.expectedToString, s.ToString()));

            Spec = "#1"
                .ArrangeNull()
                .With(new Size(1, 1), new Size(1, 1))
                .Act((_, s1, s2) => s1)
                .AssertEqualsExpected()
                .Assert("have same hash code", (s1, s2) => Assert.AreEqual(s1.GetHashCode(), s2.GetHashCode()));

            Spec = "#1 and #2"
                .ArrangeNull()
                .With(new Size(1, 1), new Size(2, 1))
                .With(new Size(2, 1), new Size(1, 1))
                .With(new Size(1, 1), new Size(1, 2))
                .Act((_, s1, s2) => Equals(s1, s2))
                .Assert("don't same hash code", (_, s1, s2) => Assert.AreNotEqual(s1.GetHashCode(), s2.GetHashCode()))
                .Assert("are not equal", (b, d) => Assert.IsFalse(b));

            Spec = "#1 not equal to null"
                .ArrangeNull()
                .With(new Size(1, 1))
                .Act((_, s) => s)
                .Assert("are not equal", (b, s) => Assert.IsFalse(Equals(null, s)))
                .Assert("are not equal", (b, s) => Assert.IsFalse(s.Equals(null)))
                .Assert("are not equal", (b, s) => Assert.IsFalse(Equals(s, null)));
        }
    }
}