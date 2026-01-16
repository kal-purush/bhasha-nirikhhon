using System.Threading.Tasks;
using Autofac;
using Moq;
using WordCount.Abstractions.HttpClient;
using WordCount.Implementations;
using WordCount.Interfaces.ArgumentsHandling;
using WordCount.Models.Parameters;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests
{
    public class TextUrlFileLoaderTests
    {
        private readonly Mock<ITextUrlParameterParser> _mockTextUrlParameterParser;
        private readonly Mock<IHttpClient> _mockHttpClient;
        private readonly TextUrlFileLoader _systemUnderTest;

        public TextUrlFileLoaderTests()
        {
            _mockTextUrlParameterParser = new Mock<ITextUrlParameterParser>();
            _mockHttpClient = new Mock<IHttpClient>();

            ContainerBuilder containerBuilder = new ContainerBuilder();

            containerBuilder
                .RegisterInstance(instance: _mockHttpClient.Object)
                .As<IHttpClient>();

            containerBuilder
                .RegisterInstance(instance: _mockTextUrlParameterParser.Object)
                .As<ITextUrlParameterParser>();

            containerBuilder
                .RegisterType<TextUrlFileLoader>();

            _systemUnderTest = containerBuilder
                .Build()
                .Resolve<TextUrlFileLoader>();
        }

        [NamedFact]
        public void TextUrlFileLoaderTests_HttpClient_returns_abc_expect_abc()
        {
            _mockTextUrlParameterParser
                .Setup(expression: m => m.ParseTextUrlParameter())
                .Returns(value: new TextUrlParameter() {IsPresent = true});

            _mockHttpClient
                .Setup(m => m.ReadString(It.IsAny<string>()))
                .Returns(value: Task.FromResult("abc"));

            string actual = _systemUnderTest.ReadTextFile();

            Assert.Equal(expected: "abc", actual: actual);
        }

        [NamedFact]
        public void TextUrlFileLoaderTests_Parameter_Url_expect_in_call_of_readString()
        {
            _mockTextUrlParameterParser
                .Setup(m => m.ParseTextUrlParameter())
                .Returns(new TextUrlParameter()
                {
                    IsPresent = true,
                    Url = "http://textfiles.rolz.org/adventure/221baker.txt"
                });

            _systemUnderTest.ReadTextFile();

            _mockHttpClient
                .Verify(
                    expression: v => v.ReadString("http://textfiles.rolz.org/adventure/221baker.txt"),
                    times: Times.Once);
        }

        [NamedFact]
        public void TextUrlFileLoaderTests_Parameter_is_not_present_expect_null()
        {
            _mockTextUrlParameterParser
                .Setup(m => m.ParseTextUrlParameter())
                .Returns(new TextUrlParameter() {IsPresent = false});

            string actual = _systemUnderTest.ReadTextFile();

            Assert.Null(@object: actual);

            _mockHttpClient
                .Verify(
                    expression: v => v.ReadString(It.IsAny<string>()),
                    times: Times.Never);
        }
    }
}