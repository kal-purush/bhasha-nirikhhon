using Bogus;
using NUnit.Framework.Constraints;
using OfficePerformanceReview.Domain.Common.ValueObjects;
using OfficePerformanceReview.Domain.PerformanceReview.Enums;
using OfficePerformanceReview.Domain.PerformanceReview.Root;

namespace OfficePerformanceReview.DomainTest.PerformanceReviewOverviewTest
{
    internal class PerformanceReviewOverviewTest : TestBase
    {
        private PerformanceReviewOverview _performanceReviewOverview;
        private DateRange _dateRange;
        public override void ExtendSetup()
        {
            base.ExtendSetup();
            // Customizing AutoFixture to generate realistic EvaluationForm instances
            Fixture.Customize<PerformanceReviewOverview>(composer => composer
                .FromFactory(() => new PerformanceReviewOverview(
                    reviewDate: new DateRange(DateOnly.FromDateTime(DateTime.Now.AddDays(-30)), DateOnly.FromDateTime(DateTime.Now)),
                    year: Faker.Date.Recent(refDate: DateTime.Now).Year.ToString(),
                    session: Enumeration.GetRandomEnumValue<Session>()
                ))
            );

            // Creating an instance using AutoFixture
            _performanceReviewOverview = Fixture.Create<PerformanceReviewOverview>();
            _dateRange = new DateRange(DateOnly.FromDateTime(DateTime.Now.AddDays(-30)), DateOnly.FromDateTime(DateTime.Now));
        }
        [Test]
        public void Constructor_ShouldInitializeCorrectly_WhenValidArgumentsArePassed()
        {
            // Arrange

            var year = DateTime.Now.Year.ToString();
            var session = Session.Second;

            // Act
            var reviewOverview = new PerformanceReviewOverview(_dateRange, year, session);

            // Assert
            reviewOverview.ReviewGuid.Should().NotBeEmpty();
            reviewOverview.ReviewDate.Should().BeEquivalentTo(_dateRange);
            reviewOverview.Year.Should().Be(year);
            reviewOverview.ReviewSession.Should().Be(session);
            reviewOverview.FeedBackGivers.Should().BeEmpty();
        }

        [Test]
        public void SetPerformanceReviewOverview_ShouldUpdateValues_WhenValidArgumentsArePassed()
        {
            // Arrange

            var newYear = DateTime.Now.Year.ToString();
            var newSession = Session.First;

            // Act
            _performanceReviewOverview.SetPerformanceReviewOverview(_dateRange, newYear, newSession);

            // Assert
            _performanceReviewOverview.ReviewDate.Should().BeEquivalentTo(_dateRange);
            _performanceReviewOverview.Year.Should().Be(newYear);
            _performanceReviewOverview.ReviewSession.Should().Be(newSession);
        }

        [Test]
        public void SetFeedBackGiver_ShouldUpdateFeedBackGivers_WhenValidListIsPassed()
        {
            // Arrange
            var feedbackGivers = new List<NameValue>
            {
                new NameValue(Faker.Random.Number(), Faker.Person.FullName),
                new NameValue(Faker.Random.Number(), Faker.Person.FullName)
            };

            // Act
            _performanceReviewOverview.SetFeedBackGiver(feedbackGivers);

            // Assert
            _performanceReviewOverview.FeedBackGivers.Should().BeEquivalentTo(feedbackGivers);
        }

        [Test]
        public void Constructor_ShouldThrowException_WhenYearIsNullOrEmpty()
        {
            // Arrange

            var session = Session.First;

            // Act & Assert
            Action act = () => new PerformanceReviewOverview(_dateRange, null, session);
            act.Should().Throw<ArgumentNullException>();

            Action actEmpty = () => new PerformanceReviewOverview(_dateRange, string.Empty, session);
            actEmpty.Should().Throw<ArgumentException>();
        }

        [Test]
        public void Constructor_ShouldThrowException_WhenReviewDateIsNull()
        {
            // Arrange
            var year = DateTime.Now.Year.ToString();
            var session = Session.Second;

            // Act & Assert
            Action act = () => new PerformanceReviewOverview(null, year, session);
            act.Should().Throw<ArgumentNullException>();
        }

        [Test]
        public void Constructor_ShouldThrowException_WhenSessionIsNull()
        {
            // Arrange

            var year = DateTime.Now.Year.ToString();

            // Act & Assert
            Action act = () => new PerformanceReviewOverview(_dateRange, year, null);
            act.Should().Throw<ArgumentNullException>();
        }
    }
}


