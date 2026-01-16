using System;
using System.Threading;
using System.Threading.Tasks;
using Dfe.Spi.Common.Logging.Definitions;
using Dfe.Spi.UkrlpAdapter.Application.Cache;
using Dfe.Spi.UkrlpAdapter.Functions.Cache;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Timers;
using Moq;
using NUnit.Framework;

namespace Dfe.Spi.UkrlpAdapter.Functions.UnitTests.Cache
{
    public class WhenDownloadingEstablishmentsOnSchedule
    {
        private Mock<ICacheManager> _cacheManagerMock;
        private Mock<ILoggerWrapper> _loggerMock;
        private DownloadProvidersScheduled _function;
        private TimerInfo _timerInfo;
        private CancellationToken _cancellationToken;

        [SetUp]
        public void Arrange()
        {
            _cacheManagerMock = new Mock<ICacheManager>();

            _loggerMock = new Mock<ILoggerWrapper>();

            _function = new DownloadProvidersScheduled(
                _cacheManagerMock.Object,
                _loggerMock.Object);

            _timerInfo = new TimerInfo(new ConstantSchedule(
                    new TimeSpan()),
                new ScheduleStatus());

            _cancellationToken = default(CancellationToken);
        }

        [Test]
        public async Task ThenItShouldDownloadEstablishmentsToCache()
        {
            await _function.Run(_timerInfo, _cancellationToken);

            _cacheManagerMock.Verify(m => m.DownloadProvidersToCacheAsync(_cancellationToken), Times.Once);
        }
    }
}