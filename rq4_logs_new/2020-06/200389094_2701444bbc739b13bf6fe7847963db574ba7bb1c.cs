using CommonDomainObjects;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Test
{
    [TestFixture]
    public class TestClassificationScheme
    {
        private static readonly IDictionary<char, IList<char>> _super = new Dictionary<char, IList<char>>
        {
            { 'A', new char[]{} },
            { 'B', new char[]{} },
            { 'C', new char[]{ 'B' } },
            { 'D', new char[]{ 'B' } },
        };

        [TestCase('A', 'A', true )]
        [TestCase('A', 'B', false)]
        [TestCase('A', 'C', false)]
        [TestCase('A', 'D', false)]
        [TestCase('B', 'A', false)]
        [TestCase('B', 'B', true )]
        [TestCase('B', 'C', true )]
        [TestCase('B', 'D', true )]
        [TestCase('C', 'A', false)]
        [TestCase('C', 'B', false)]
        [TestCase('C', 'C', true )]
        [TestCase('C', 'D', false)]
        [TestCase('D', 'A', false)]
        [TestCase('D', 'B', false)]
        [TestCase('D', 'C', false)]
        [TestCase('D', 'D', true )]
        public void Contains(
            char lhs,
            char rhs,
            bool contains
            )
        {
            var map = _super.Keys.ToDictionary(
                vertex => vertex,
                vertex => new Class(
                    Guid.Empty,
                    vertex.ToString()));
            var classificationScheme = new ClassificationScheme(_super.Select(c => map[c]));
            Assert.That(classificationScheme[map[lhs]].Contains(classificationScheme[map[rhs]]), Is.EqualTo(contains));
        }

        [Test]
        public void NewClassificationScheme()
        {
            var super = _super.Select(
                vertex => new Class(
                    Guid.Empty,
                    vertex.ToString()));

            var classificationScheme = new ClassificationScheme(super);
            Assert.That(classificationScheme.Classes.Count, Is.EqualTo(_super.Count));
            foreach(var @class in super.Keys)
            {
                var classificationSchemeClass = classificationScheme[@class];
                Assert.That(classificationSchemeClass      , Is.Not.Null     );
                Assert.That(classificationSchemeClass.Class, Is.EqualTo(@class));

                var superclass = super[@class].FirstOrDefault();
                var superClassificationClass = classificationScheme[superclass];

                Assert.That(classificationSchemeClass.Super, Is.EqualTo(superClassificationClass));

                if(superClassificationClass != null)
                {
                    Assert.That(superClassificationClass.Sub, Has.Member(classificationSchemeClass));
                    Assert.That(superClassificationClass.Interval.Contains(classificationSchemeClass.Interval));
                }
            }

            foreach(var classificationSchemeClass in classificationScheme.Classes)
                Assert.That(super.ContainsKey(classificationSchemeClass.Class), Is.True);
        }
    }
}