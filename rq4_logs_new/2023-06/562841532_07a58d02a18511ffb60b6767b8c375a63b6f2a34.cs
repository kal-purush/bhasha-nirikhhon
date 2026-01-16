using Booking_Movie.Data.Repositories;
using Booking_Movie.ViewModel.Catalog.ReportVM.BookingReportVM;
using Booking_Movie.ViewModel.Enums.Movie;
using Microsoft.Extensions.DependencyInjection;

namespace BookingMovie.UnitTest.RepositoryTest
{
    public class BookingRepositoryTest : IClassFixture<StartupFixture>
    {
        private readonly BookingRepository _bookingRepository;

        public BookingRepositoryTest(StartupFixture fixture)
        {
            _bookingRepository = fixture.ServiceProvider.GetService<BookingRepository>();
        }

        [Fact]
        public void ScanningRateReportChart()
        {
            var request = new ScanningRateChartReportRequest()
            {
                Conditional = MovieRevenueChartReportEnum.Year
            };
            var result = _bookingRepository.ScanningRateReportChart(request);
        }
    }
}