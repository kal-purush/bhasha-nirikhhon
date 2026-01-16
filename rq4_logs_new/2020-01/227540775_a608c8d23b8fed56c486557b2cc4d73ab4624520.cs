using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture.NUnit3;
using Dfe.Spi.Common.Logging.Definitions;
using Dfe.Spi.UkrlpAdapter.Application.Cache;
using Dfe.Spi.UkrlpAdapter.Domain.Cache;
using Dfe.Spi.UkrlpAdapter.Domain.Events;
using Dfe.Spi.UkrlpAdapter.Domain.Mapping;
using Dfe.Spi.UkrlpAdapter.Domain.UkrlpApi;
using Moq;
using NUnit.Framework;

namespace Dfe.Spi.GiasAdapter.Application.UnitTests.Cache
{
    public class WhenDownloadingEstablishmentsToCache
    {
        private Mock<IStateRepository> _stateRepositoryMock;
        private Mock<IUkrlpApiClient> _ukrlpApiClientMock;
        private Mock<IProviderRepository> _providerRepositoryMock;
        private Mock<IMapper> _mapperMock;
        private Mock<IEventPublisher> _eventPublisherMock;
        private Mock<IProviderProcessingQueue> _providerProcessingQueueMock;
        private Mock<ILoggerWrapper> _loggerMock;
        private CacheManager _manager;
        private CancellationToken _cancellationToken;

        [SetUp]
        public void Arrange()
        {
            _stateRepositoryMock = new Mock<IStateRepository>();

            _ukrlpApiClientMock = new Mock<IUkrlpApiClient>();
            _ukrlpApiClientMock.Setup(c =>
                    c.GetProvidersUpdatedSinceAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new Provider[0]);

            _providerRepositoryMock = new Mock<IProviderRepository>();

            _mapperMock = new Mock<IMapper>();

            _eventPublisherMock = new Mock<IEventPublisher>();

            _providerProcessingQueueMock = new Mock<IProviderProcessingQueue>();

            _loggerMock = new Mock<ILoggerWrapper>();

            _manager = new CacheManager(
                _stateRepositoryMock.Object,
                _ukrlpApiClientMock.Object,
                _providerRepositoryMock.Object,
                _mapperMock.Object,
                _eventPublisherMock.Object,
                _providerProcessingQueueMock.Object,
                _loggerMock.Object);

            _cancellationToken = new CancellationToken();
        }

        [Test]
        public async Task ThenItShouldLastReadTimeFromRepository()
        {
            await _manager.DownloadProvidersToCacheAsync(_cancellationToken);

            _stateRepositoryMock.Verify(r => r.GetLastProviderReadTimeAsync(_cancellationToken), Times.Once);
        }

        [Test, AutoData]
        public async Task ThenItShouldGetProvidersFromUkrlp(DateTime lastUpdate)
        {
            _stateRepositoryMock.Setup(r => r.GetLastProviderReadTimeAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(lastUpdate);

            await _manager.DownloadProvidersToCacheAsync(_cancellationToken);

            _ukrlpApiClientMock.Verify(c => c.GetProvidersUpdatedSinceAsync(lastUpdate, _cancellationToken),
                Times.Once);
        }

        [Test, AutoData]
        public async Task ThenItShouldStoreProvidersInStaging(Provider[] providers)
        {
            _ukrlpApiClientMock.Setup(c =>
                    c.GetProvidersUpdatedSinceAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(providers);

            await _manager.DownloadProvidersToCacheAsync(_cancellationToken);

            _providerRepositoryMock.Verify(r => r.StoreInStagingAsync(providers, _cancellationToken),
                Times.Once);
        }

        [Test]
        public async Task ThenItShouldQueueBatchesOfUkprnsForProcessing()
        {
            var providers = new Provider[150];
            for (var i = 0; i < providers.Length; i++)
            {
                providers[i] = new Provider
                {
                    UnitedKingdomProviderReferenceNumber = 1000001 + i,
                };
            }

            _ukrlpApiClientMock.Setup(c =>
                    c.GetProvidersUpdatedSinceAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(providers);

            await _manager.DownloadProvidersToCacheAsync(_cancellationToken);

            var expectedBatch1 = providers.Take(100).Select(e => e.UnitedKingdomProviderReferenceNumber).ToArray();
            var expectedBatch2 = providers.Skip(100).Take(100).Select(e => e.UnitedKingdomProviderReferenceNumber)
                .ToArray();
            _providerProcessingQueueMock.Verify(q => q.EnqueueBatchOfStagingAsync(
                    It.Is<long[]>(ukprns => AreEqual(expectedBatch1, ukprns)), _cancellationToken),
                Times.Once);
            _providerProcessingQueueMock.Verify(q => q.EnqueueBatchOfStagingAsync(
                    It.Is<long[]>(ukprns => AreEqual(expectedBatch2, ukprns)), _cancellationToken),
                Times.Once);
        }

        [Test]
        public async Task ThenItShouldUpdateLastReadTimeInRepository()
        {
            await _manager.DownloadProvidersToCacheAsync(_cancellationToken);

            _stateRepositoryMock.Verify(r => r.SetLastProviderReadTimeAsync(
                    It.Is<DateTime>(dt => dt >= DateTime.Now.AddSeconds(-1)), _cancellationToken),
                Times.Once);
        }

        private bool AreEqual(long[] expected, long[] actual)
        {
            // Null check
            if (expected == null && actual == null)
            {
                return true;
            }

            if (expected == null || actual == null)
            {
                return false;
            }

            // Length check
            if (expected.Length != actual.Length)
            {
                return false;
            }

            // Item check
            for (var i = 0; i < expected.Length; i++)
            {
                if (expected[i] != actual[i])
                {
                    return false;
                }
            }

            // All good
            return true;
        }
    }
}