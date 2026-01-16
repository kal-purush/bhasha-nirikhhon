using Xunit;

namespace HiHelloWebAPI.Controllers.Tests
{
    public class HelloFixtures
    {
        [Fact]
        public void Hello_Controller_Checks()
        {
            HiHelloController HelloControllerObject = new HiHelloController();

            Assert.Equal("append \"hi\" or \"hello\" in the url to get response",
                HelloControllerObject.Get());
            Assert.Equal("error", HelloControllerObject.Get(""));
            Assert.Equal("hi", HelloControllerObject.Get("hello"));
        }
    }
}