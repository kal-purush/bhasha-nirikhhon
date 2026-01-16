namespace CSL.Tests;

using CSL;
using CSL.Exceptions;
using NUnit.Framework;
using Antlr4.Runtime;
using System;
using System.IO;

[TestFixture]
public class DescriptionTests
{
    [TestCase("\"Simple description\"", "Simple description")]
    [TestCase("\"This is a test\"", "This is a test")]
    [TestCase("\"Description with numbers 123\"", "Description with numbers 123")]
    [TestCase("\"Special characters: !@#$%^&*()\"", "Special characters: !@#$%^&*()")]
    public void TestLiteralDescription(string input, string expectedText)
    {
        var description = DescriptionParser.ParseDescription(input);

        Assert.That(description, Is.Not.Null);
        Assert.That(description.Text, Is.EqualTo(expectedText));
    }

    [TestCase("Description without quotes")]
    [TestCase("'Wrong quote type'")]
    [TestCase("\"Unclosed quote")]
    [TestCase("\"Contains \\backslash\"")]
    [TestCase("")]
    public void TestInvalidDescriptionLiterals(string input)
    {
        Assert.Throws<InvalidLiteralCompilerException>(() => DescriptionParser.ParseDescription(input));
    }
}