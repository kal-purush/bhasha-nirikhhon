using Autofac;
using Moq;
using System;
using System.Linq;
using WordCount.Abstractions.Environment;
using WordCount.Implementations.ArgumentsHandling;
using WordCount.Models.Parameters;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests.ArgumentsHandlingTests
{
    public class TextUrlParameterParserTests
    {
        private readonly Mock<IEnvironment> _mockEnvironment;
        private readonly TextUrlParameterParser _systemUnderTest;

        public TextUrlParameterParserTests()
        {
            _mockEnvironment = new Mock<IEnvironment>();

            ContainerBuilder containerBuilder = new ContainerBuilder();

            containerBuilder
                .RegisterInstance(instance: _mockEnvironment.Object)
                .As<IEnvironment>();

            containerBuilder
                .RegisterType<TextUrlParameterParser>();

            _systemUnderTest = containerBuilder
                .Build()
                .Resolve<TextUrlParameterParser>();
        }

        [NamedFact]
        public void TextUrlParameterParserTests_Args_is_empty_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(m => m.GetCommandLineArgs())
                .Returns(Array.Empty<string>());

            TextUrlParameter actual = _systemUnderTest.ParseTextUrlParameter();

            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void TextUrlParameterParserTests_Args_is_null_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(m => m.GetCommandLineArgs())
                .Returns(value: null);

            TextUrlParameter actual = _systemUnderTest.ParseTextUrlParameter();

            Assert.False(condition: actual.IsPresent);
        }

        [NamedFact]
        public void TextUrlParameterParserTests_Args_has_texturl_equalsign_wwwaddress_Expect_IsPresent_True()
        {
            _mockEnvironment
                .Setup(m => m.GetCommandLineArgs())
                .Returns(value: new string[] { "-texturl=http://www.google.de" });

            TextUrlParameter actual = _systemUnderTest.ParseTextUrlParameter();

            Assert.True(condition: actual.IsPresent);
        }

        [NamedFact]
        public void TextUrlParameterParserTests_Args_has_texturl_equalsign_invalid_wwwaddress_Expect_IsPresent_False()
        {
            _mockEnvironment
                .Setup(m => m.GetCommandLineArgs())
                .Returns(value: new string[] { "-texturl=www.googlede" });

            TextUrlParameter actual = _systemUnderTest.ParseTextUrlParameter();

            Assert.False(condition: actual.IsPresent);
        }
    }
}