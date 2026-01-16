using djfoxer.PiHole.AdFilterSet.Checker;
using djfoxer.PiHole.AdFilterSet.Configure;
using djfoxer.PiHole.AdFilterSet.Exceptions;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;

namespace djfoxer.PiHole.AdFilterSet.Tests
{
    public class AdFilterSetCheckerTests
    {
        [Theory]
        [InlineData("TestSource1.txt")]
        public async Task CheckAdFilterSet_DetectAllFlawsInFile(string fileText)
        {
            //arrange
            var pathUrl = DirectoryHelper.FindPathToParentFile(fileText);
            var httpClientFactory = new ServiceCollection()
                .AddHttpClient()
                .BuildServiceProvider()
                .GetService<IHttpClientFactory>();
            var checker = new AdFilterSetChecker(A.Fake<ILogger<AdFilterSetChecker>>(), httpClientFactory);
            //act
            var checkData = await checker.Check(pathUrl);
            //assert
            Assert.Equal(6, checkData.ValidationInfoItems.Count);
            Assert.Equal(5, checkData.ValidationInfoItems.Where(x => x.Exception != null).Count());

            Assert.IsType<MissingDataException>(checkData.ValidationInfoItems.ElementAt(0).Exception);
            Assert.Equal("https://www.djfoxer.pl/aaa.txt", checkData.ValidationInfoItems.ElementAt(0).Url);

            Assert.IsType<WrongUrlException>(checkData.ValidationInfoItems.ElementAt(1).Exception);
            Assert.Equal("aaaaaaaaaaaaa", checkData.ValidationInfoItems.ElementAt(1).Url);

            Assert.IsType<EmptyLineException>(checkData.ValidationInfoItems.ElementAt(2).Exception);
            Assert.Equal("", checkData.ValidationInfoItems.ElementAt(2).Url);

            Assert.IsType<DuplicateLineException>(checkData.ValidationInfoItems.ElementAt(4).Exception);
            Assert.Equal("https://raw.githubusercontent.com/djfoxer/pihole-adfilter-set/main/adfilterset.txt", checkData.ValidationInfoItems.ElementAt(4).Url);

            Assert.IsType<MissingHostException>(checkData.ValidationInfoItems.ElementAt(5).Exception);
            Assert.Equal("https://wwwwwwww.djfoxer.pl/", checkData.ValidationInfoItems.ElementAt(5).Url);
        }
    }
}