namespace OctoStyle.Core.Tests.Integration
{
    using System.IO;
    using System.Reflection;

    using NUnit.Framework;

    [TestFixture]
    public class CodeAnalyzerTests
    {
        [Test]
        public void AnalyzeShouldReturnAllAnalysisIssues()
        {
            var projectPath = Path.GetDirectoryName(
				Path.GetDirectoryName(
					Path.GetDirectoryName(
						Assembly.GetExecutingAssembly().Location)));

            var styleCop = new CodeAnalyzer();
            var analysis = styleCop.Analyze(projectPath, Path.Combine(projectPath, "StyleCopTests.cs"));
        }
    }
}