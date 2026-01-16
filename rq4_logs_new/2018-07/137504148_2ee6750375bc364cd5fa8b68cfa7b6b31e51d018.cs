using Autofac;
using Moq;
using WordCount.Abstractions.Console;
using WordCount.Implementations.ArgumentsHandling;
using WordCount.Models;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests.ArgumentsHandlingTests
{
    public class StopwordListParameterParserTests
    {
        private readonly Mock<IEnvironment> _mockEnvironment;
        private readonly StopwordListParameterParser _systemUnderTest;

        public StopwordListParameterParserTests()
        {
            _mockEnvironment = new Mock<IEnvironment>();

            ContainerBuilder containerBuilder = new ContainerBuilder();

            containerBuilder
                .RegisterInstance(instance: _mockEnvironment.Object)
                .As<IEnvironment>();

            containerBuilder
                .RegisterType<StopwordListParameterParser>();

            _systemUnderTest = containerBuilder
                .Build()
                .Resolve<StopwordListParameterParser>();
        }

        [NamedFact]
        public void StopwordListParameterParserTests_Args_have_no_Dictionary_Parameter_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new[] { "bla.txt" });

            StopwordListParameter actual = _systemUnderTest
                .ParseStopwordListParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void StopwordListParameterParserTests_Args_is_null_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: null);

            StopwordListParameter actual = _systemUnderTest
                .ParseStopwordListParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void StopwordListParameterParserTests_Args_is_empty_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(expression: m => m.GetCommandLineArgs())
                .Returns(value: new string[0]);

            StopwordListParameter actual = _systemUnderTest
                .ParseStopwordListParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void StopwordListParameterParserTests_Args_has_StopwordListParameter_with_no_equal_sign_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(m => m.GetCommandLineArgs())
                .Returns(new[] { "-stopwordlist" });

            StopwordListParameter actual = _systemUnderTest
                .ParseStopwordListParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void StopwordListParameterParserTests_Args_has_StopwordListParameter_Without_File_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(m => m.GetCommandLineArgs())
                .Returns(new[] { "-stopwordlist=" });

            StopwordListParameter actual = _systemUnderTest
                .ParseStopwordListParameter();

            Assert.NotNull(@object: actual);
            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void StopwordListParameterParserTests_Args_has_StopwordListParameter_With_File_bla_txt_Expect_FileName_bla_txt()
        {
            _mockEnvironment
                .Setup(m => m.GetCommandLineArgs())
                .Returns(new[] { "-stopwordlist=bla.txt" });

            StopwordListParameter actual = _systemUnderTest
                .ParseStopwordListParameter();

            Assert.NotNull(@object: actual);
            Assert.Equal(expected: "bla.txt", actual: actual.FileName);
        }
    }
}