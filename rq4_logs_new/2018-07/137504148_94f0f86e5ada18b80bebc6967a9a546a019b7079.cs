using System;
using System.Globalization;
using System.Linq;
using Autofac;
using Moq;
using WordCount.Implementations;
using WordCount.Interfaces;
using WordCount.Interfaces.Language;
using WordCount.Models.Results;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests
{
    public class StatisticsOutputTests
    {
        private readonly Mock<IDisplayOutput> _mockDisplayOutput;
        private readonly Mock<ILanguageResource> _mockLanguageResource;
        private readonly Mock<ILanguageDecision> _mockLanguageDecision;
        private readonly StatisticsOutput _systemUnderTest;

        public StatisticsOutputTests()
        {
            _mockDisplayOutput = new Mock<IDisplayOutput>();
            _mockLanguageDecision = new Mock<ILanguageDecision>();
            _mockLanguageResource = new Mock<ILanguageResource>();

            ContainerBuilder containerBuilder = new ContainerBuilder();

            containerBuilder
                .RegisterInstance(instance: _mockDisplayOutput.Object)
                .As<IDisplayOutput>();

            containerBuilder
                .RegisterInstance(instance: _mockLanguageDecision.Object)
                .As<ILanguageDecision>();

            containerBuilder
                .RegisterInstance(instance: _mockLanguageResource.Object)
                .As<ILanguageResource>();

            containerBuilder
                .RegisterType<StatisticsOutput>();

            _systemUnderTest = containerBuilder
                .Build()
                .Resolve<StatisticsOutput>();
        }

        [NamedFact]
        public void StatisticsOutputTests_WriteNumberOfWords()
        {
            _mockLanguageResource
                .Setup(m => m.GetResourceStringById("NUMBER_OF_WORDS"))
                .Returns("Anzahl Wörter");

            _systemUnderTest.WriteNumberOfWords(
                numberOfWords: 2,
                maxCountOfFillingPoints: 20);

            _mockDisplayOutput
                .Verify(v => v.WriteLine("- Anzahl Wörter....... 2"), Times.Once);
        }

        [NamedFact]
        public void StatisticsOutputTests_WriteNumberOfUniqeWords()
        {
            _mockLanguageResource
                .Setup(m => m.GetResourceStringById("UNIQUE"))
                .Returns("Eindeutig");

            _systemUnderTest.WriteNumberOfUniqeWords(
                numberOfUniqeWords: 2,
                maxCountOfFillingPoints: 20);

            _mockDisplayOutput
                .Verify(v => v.WriteLine("- Eindeutig........... 2"), Times.Once);
        }

        [NamedFact]
        public void StatisticsOutputTests_WriteAverageWordLength()
        {
            _mockLanguageResource
                .Setup(m => m.GetResourceStringById("AVERAGE_WORD_LENGTH"))
                .Returns("Durchschnittliche Wortlänge");

            _mockLanguageResource
                .Setup(m => m.GetResourceStringById("CHARACTERS"))
                .Returns("Zeichen");

            _mockLanguageDecision
                .Setup(m => m.DecideLanguage())
                .Returns(new DecideLanguageResult {Culture = CultureInfo.GetCultureInfo("de-DE")});

            _systemUnderTest.WriteAverageWordLength(
                averageWordLength: 2.52,
                maxCountOfFillingPoints: 30);

            _mockDisplayOutput
                .Verify(v => v.WriteLine("- Durchschnittliche Wortlänge... 2,52 Zeichen"), Times.Once);
        }

        [NamedFact]
        public void StatisticsOutputTests_WriteNumberOfChapters()
        {
            _mockLanguageResource
                .Setup(m => m.GetResourceStringById("CHAPTERS"))
                .Returns("Kapitel");

            _systemUnderTest.WriteNumberOfChapters(
                numberOfChapters: 3,
                maxCountOfFillingPoints: 20);

            _mockDisplayOutput
                .Verify(v => v.WriteLine("- Kapitel............. 3"), Times.Once);
        }
    }
}