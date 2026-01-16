using System;
using System.Linq;
using Autofac;
using Moq;
using WordCount.Abstractions.Console;
using WordCount.Implementations.ArgumentsHandling;
using WordCount.Models;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests.ArgumentsHandlingTests
{
    public class SourceFileParameterParserTests
    {
        private readonly Mock<IEnvironment> _mockEnvironment;
        private readonly SourceFileParameterParser _systemUnderTest;

        public SourceFileParameterParserTests()
        {
            _mockEnvironment = new Mock<IEnvironment>();

            var containerBuilder = new ContainerBuilder();

            containerBuilder
                .RegisterInstance(instance: _mockEnvironment.Object)
                .As<IEnvironment>();

            containerBuilder
                .RegisterType<SourceFileParameterParser>();

            _systemUnderTest = containerBuilder
                .Build()
                .Resolve<SourceFileParameterParser>();
        }

        [NamedFact]
        public void SourceFileParameterParserTests_Args_is_empty_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new string[0]);

            SourceFileParameter actual = _systemUnderTest.ParseSourceFileParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void SourceFileParameterParserTests_Args_is_null_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: null);

            SourceFileParameter actual = _systemUnderTest.ParseSourceFileParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void SourceFileParameterParserTests_Args_has_FileName_Expect_Is_Present_True()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new[] { "bla.txt" });

            SourceFileParameter actual = _systemUnderTest.ParseSourceFileParameter();

            Assert.NotNull(@object: actual);
            Assert.True(condition: actual.IsPresent);
        }

        [NamedFact]
        public void SourceFileParameterParserTests_Args_has_FileName_bla_txt_Expect_Is_FileName_bla_txt()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new[] { "bla.txt" });

            SourceFileParameter actual = _systemUnderTest.ParseSourceFileParameter();

            Assert.NotNull(@object: actual);
            Assert.Equal(
                expected: "bla.txt",
                actual: actual.FileName);
        }
    }
}