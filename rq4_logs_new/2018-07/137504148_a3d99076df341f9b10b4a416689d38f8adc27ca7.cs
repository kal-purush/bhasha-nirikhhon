using Autofac;
using Moq;
using WordCount.Abstractions.Console;
using WordCount.Implementations.Language;
using WordCount.Interfaces;
using WordCount.Interfaces.ArgumentsHandling;
using WordCount.Models.Parameters;
using WordCount.Models.Results;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests.Language
{
    public class LanguageDecisionTests
    {
        private readonly Mock<IAppSettingsReader> _mockAppSettingsReader;
        private readonly Mock<ILanguageParameterParser> _mockLanguageParameterParser;
        private readonly Mock<IConsole> _mockConsole;
        private readonly LanguageDecision _systemUnderTest;

        public LanguageDecisionTests()
        {
            _mockAppSettingsReader = new Mock<IAppSettingsReader>();
            _mockLanguageParameterParser = new Mock<ILanguageParameterParser>();
            _mockConsole = new Mock<IConsole>();

            ContainerBuilder containerBuilder = new ContainerBuilder();

            containerBuilder
                .RegisterInstance(instance: _mockAppSettingsReader.Object)
                .As<IAppSettingsReader>();

            containerBuilder
                .RegisterInstance(instance: _mockLanguageParameterParser.Object)
                .As<ILanguageParameterParser>();

            containerBuilder
                .RegisterInstance(instance: _mockConsole.Object)
                .As<IConsole>();

            containerBuilder
                .RegisterType<LanguageDecision>();

            _systemUnderTest = containerBuilder
                .Build()
                .Resolve<LanguageDecision>();
        }

        [NamedFact]
        public void LanguageDecisionTests_Parameter_is_present_and_language_de_expect_de()
        {
            _mockLanguageParameterParser
                .Setup(expression: m => m.ParseLanguageParameter())
                .Returns(value: new LanguageParameter { IsPresent = true, Language = "de" });

            DecideLanguageResult actual = _systemUnderTest.DecideLanguage();

            Assert.NotNull(@object: actual);
            Assert.Equal(expected: "de", actual: actual.Language);
        }

        [NamedFact]
        public void LanguageDecisionTests_Parameter_is_not_present_and_app_settings_has_no_value_expect_en()
        {
            _mockLanguageParameterParser
                .Setup(expression: m => m.ParseLanguageParameter())
                .Returns(value: new LanguageParameter { IsPresent = false });

            _mockAppSettingsReader
                .SetupGet(m => m.DefaultLanguage)
                .Returns(value: null);

            DecideLanguageResult actual = _systemUnderTest.DecideLanguage();

            Assert.NotNull(@object: actual);
            Assert.Equal(expected: "en", actual: actual.Language);
        }

        [NamedFact]
        public void LanguageDecisionTests_Parameter_is_not_present_and_app_settings_has_de_expect_de()
        {
            _mockLanguageParameterParser
                .Setup(expression: m => m.ParseLanguageParameter())
                .Returns(value: new LanguageParameter { IsPresent = false });

            _mockAppSettingsReader
                .SetupGet(m => m.DefaultLanguage)
                .Returns(value: "de");

            DecideLanguageResult actual = _systemUnderTest.DecideLanguage();

            Assert.NotNull(@object: actual);
            Assert.Equal(expected: "de", actual: actual.Language);
        }

        [NamedFact]
        public void LanguageDecisionTests_Parameter_is_present_and_has_language_it_expect_en_and_console_not_supported_language()
        {
            _mockLanguageParameterParser
                .Setup(m => m.ParseLanguageParameter())
                .Returns(new LanguageParameter { IsPresent = true, Language = "it" });

            DecideLanguageResult actual = _systemUnderTest.DecideLanguage();

            Assert.NotNull(@object: actual);
            Assert.Equal("en", actual.Language);

            
            _mockConsole
                .Verify(v => v.WriteLine("Language \"it\" not supported."), Times.Once);
        }

        [NamedFact]
        public void LanguageDecisionTests_appsetting_has_default_language_it_expect_en_and_console_not_supported_language()
        {
            _mockLanguageParameterParser
                .Setup(m => m.ParseLanguageParameter())
                .Returns(new LanguageParameter { IsPresent = false });

            _mockAppSettingsReader
                .SetupGet(m => m.DefaultLanguage)
                .Returns("it");

            DecideLanguageResult actual = _systemUnderTest.DecideLanguage();

            Assert.NotNull(@object: actual);
            Assert.Equal("en", actual.Language);

            _mockConsole
                .Verify(v => v.WriteLine("Language \"it\" not supported."), Times.Once);
        }



        [NamedFact]
        public void LanguageDecisionTests_parameter_has_it_appsetting_has_es_expect_en_and_console_not_supported_language_it()
        {
            _mockLanguageParameterParser
                .Setup(m => m.ParseLanguageParameter())
                .Returns(new LanguageParameter { IsPresent = true, Language = "it" });

            _mockAppSettingsReader
                .SetupGet(m => m.DefaultLanguage)
                .Returns("es");

            DecideLanguageResult actual = _systemUnderTest.DecideLanguage();

            Assert.NotNull(@object: actual);
            Assert.Equal("en", actual.Language);

            _mockConsole
                .Verify(v => v.WriteLine("Language \"it\" not supported."), Times.Once);
        }
    }
}