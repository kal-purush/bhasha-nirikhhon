// -----------------------------------------------------------------------
// <copyright file="OptionsParsing.cs" company="Ace Olszowka">
//  Copyright (c) Ace Olszowka 2020. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

namespace MsBuildConvertToProjectReference.Tests
{
    using NUnit.Framework;

    [TestFixture]
    public class OptionsParsing
    {
        [Test]
        public void TestOptionsParse()
        {
            string[] testarguments = new string[] { "Test.csproj", @"-ld=S:\Source\Tech1\", @"-lookupdirectory=S:\Source\Tech2\" };

            MSBCTPROptions actual = Program.ParseForOptions(testarguments);

            Assert.That(actual.TargetDirectory, Is.EqualTo("Test.csproj"));
            Assert.That(actual.LookupDirectories, Is.EquivalentTo(new string[] { @"S:\Source\Tech1\", @"S:\Source\Tech2\" }));
        }
    }
}