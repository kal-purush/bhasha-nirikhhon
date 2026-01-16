using Autofac;
using Moq;
using WordCount.Abstractions.Environment;
using WordCount.Implementations.ArgumentsHandling;
using WordCount.Models.Parameters;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests.ArgumentsHandlingTests
{
    public class HelpParameterParserTests
    {
        private readonly Mock<IEnvironment> _mockEnvironment;
        private readonly HelpParameterParser _systemUnderTest;

        public HelpParameterParserTests()
        {
            _mockEnvironment = new Mock<IEnvironment>();

            ContainerBuilder containerBuilder = new ContainerBuilder();

            containerBuilder
                .RegisterInstance(instance: _mockEnvironment.Object)
                .As<IEnvironment>();

            containerBuilder
                .RegisterType<HelpParameterParser>();

            _systemUnderTest = containerBuilder
                .Build()
                .Resolve<HelpParameterParser>();
        }

        [NamedFact]
        public void HelpParameterParserTests_args_is_empty_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new string[0]);

            HelpParameter actual = _systemUnderTest.ParseHelpParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void HelpParameterParserTests_args_is_null_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: null);

            HelpParameter actual = _systemUnderTest.ParseHelpParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void HelpParameterParserTests_args_have_sourcefile_but_no_help_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new[] {"mytext.txt"});

            HelpParameter actual = _systemUnderTest.ParseHelpParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void HelpParameterParserTests_args_have_help_Expect_IsPresent_True()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new[] { "-help" });

            HelpParameter actual = _systemUnderTest.ParseHelpParameter();

            Assert.NotNull(@object: actual);
            Assert.True(condition: actual.IsPresent);
        }

        [NamedFact]
        public void HelpParameterParserTests_args_have_h_Expect_IsPresent_True()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new[] { "-h" });

            HelpParameter actual = _systemUnderTest.ParseHelpParameter();

            Assert.NotNull(@object: actual);
            Assert.True(condition: actual.IsPresent);
        }
    }
}