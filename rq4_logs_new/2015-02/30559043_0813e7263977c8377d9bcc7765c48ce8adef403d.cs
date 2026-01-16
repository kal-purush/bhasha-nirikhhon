using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheckUtils;
using Microsoft.FSharp.Core;
using NUnit.Framework;

namespace DontShrinkTests
{
    using Property = Gen<Rose<Result>>;
    
    [TestFixture]
    class DontShrinkTests
    {
        private static readonly Config Config =
            Config.QuickThrowOnFailure
                .WithStartSize(50)
                .WithEveryShrink(Config.Verbose.EveryShrink);

        private static readonly Configuration Configuration = Config.ToConfiguration();

        private static bool PropertyImplementation(IList<int> xs)
        {
            return xs.Reverse().SequenceEqual(xs);
        }

        [Test]
        public void ArbFrom_WillShrinkUsingTheDefaultShrinkerForTheGivenType()
        {
            var arb = Arb.from<IList<int>>();
            var body = FSharpFunc<IList<int>, bool>.FromConverter(PropertyImplementation);
            var property = Prop.forAll(arb, body);
            Check.One(Config, property);
        }

        [Test]
        public void ArbFromGen_WillNotShrinkDueToTheDefaultShrinkerThatReturnsAnEmptySequence()
        {
            var gen = Arb.generate<IList<int>>();
            var arb = Arb.fromGen(gen);
            var body = FSharpFunc<IList<int>, bool>.FromConverter(PropertyImplementation);
            var property = Prop.forAll(arb, body);
            Check.One(Config, property);
        }

        [Test]
        public void SpecFor_WillShrinkUsingTheDefaultShrinkerForTheGivenType()
        {
            Spec
                .For(Any.OfType<IList<int>>(), xs => PropertyImplementation(xs))
                .Check(Configuration);
        }

        [Test]
        public void SpecFor_CanBeForcedToNotShrink()
        {
            Spec
                .For(Any.OfType<IList<int>>(), xs => PropertyImplementation(xs))
                .Shrink(xs => Enumerable.Empty<IList<int>>())
                .Check(Configuration);
        }

        [FsCheck.NUnit.Property]
        public Property PropertyWithRegularParam_WillShrinkUsingTheDefaultShrinkerForTheGivenType(IList<int> xsParam)
        {
            return Spec
                .For(Any.Value(xsParam), xs => PropertyImplementation(xs))
                .Build();
        }

        [FsCheck.NUnit.Property]
        public Property PropertyWithExplicitDontShrinkParam_WillNotShrink(DontShrink<IList<int>> xsParam)
        {
            return Spec
                .For(Any.Value(xsParam), xs => PropertyImplementation(xs.Item))
                .Build();
        }
    }
}