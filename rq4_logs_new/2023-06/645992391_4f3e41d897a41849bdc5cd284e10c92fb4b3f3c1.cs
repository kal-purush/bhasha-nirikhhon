using Xunit;

namespace BackEndFucntions
{
    public class DataRetrievalTest : IClassFixture<DataRetrieval>
    {
        private readonly DataRetrieval _dataRetrieval;
        public DataRetrievalTest(DataRetrieval dataRetrieval)
        {
            _dataRetrieval = dataRetrieval;
        }
        [Theory]
        [InlineData("transport")]
        [InlineData("food")]
        [InlineData("utility")]
        public async Task EmptyTransportRetrieval(string ACTIVITY_TYPE)
        {
            const int USER_ID = 123456789;
            DateTime TODAY = DateTime.UtcNow;
            List<dynamic> EXPECTED_RESULT = null;
            List<dynamic> result = _dataRetrieval.RetrieveCertainType(USER_ID, TODAY, ACTIVITY_TYPE);
            Assert.Equal(EXPECTED_RESULT, result);
        }
    }
}