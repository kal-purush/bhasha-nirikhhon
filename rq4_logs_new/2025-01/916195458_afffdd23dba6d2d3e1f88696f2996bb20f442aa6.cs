using ExploreGetRssFeed.Data;
using System.ComponentModel.DataAnnotations;

namespace ExploreGetRssFeed.Tests.Models
{
    public class FeedEntryDataModelTests
    {
        [Fact]
        public void Create_WithValidParameters_ShouldReturnFeedEntryDataModel()
        {
            // Arrange
            string title = "Test Title";
            string webAddress = "https://example.com";
            string routeName = "TestRoute";

            // Act
            var feedEntry = new FeedEntryDataModel
            {
                Title = title,
                WebAddress = webAddress,
                RouteName = routeName
            };

            // Assert
            Assert.Equal("Test Title", feedEntry.Title);
            Assert.Equal("https://example.com", feedEntry.WebAddress);
            Assert.Equal("TestRoute", feedEntry.RouteName);
        }

        [Fact]
        public void Title_WithMaxLength_ShouldNotThrowException()
        {
            // Arrange
            string title = new string('a', 100);
            var feedEntry = new FeedEntryDataModel
            {
                Title = title,
                WebAddress = "https://example.com",
                RouteName = "TestRoute"
            };

            // Act & Assert
            Assert.Equal(100, feedEntry.Title.Length);
        }

        [Fact]
        public void WebAddress_WithMaxLength_ShouldNotThrowException()
        {
            // Arrange
            string webAddress = new string('a', 200);
            var feedEntry = new FeedEntryDataModel
            {
                Title = "Test Title",
                WebAddress = webAddress,
                RouteName = "TestRoute"
            };

            // Act & Assert
            Assert.Equal(200, feedEntry.WebAddress.Length);
        }

        [Fact]
        public void RouteName_WithMaxLength_ShouldNotThrowException()
        {
            // Arrange
            string routeName = new string('a', 100);
            var feedEntry = new FeedEntryDataModel
            {
                Title = "Test Title",
                WebAddress = "https://example.com",
                RouteName = routeName
            };

            // Act & Assert
            Assert.Equal(100, feedEntry.RouteName.Length);
        }

        // todo: Move to UI/UX test
        [Fact(Skip ="true")]
        public void Title_WithExceedingMaxLength_ShouldThrowException()
        {
            // Arrange
            string title = new string('a', 101);

            // Act & Assert
            Assert.Throws<ValidationException>(() =>
            {
                var feedEntry = new FeedEntryDataModel
                {
                    Title = title,
                    WebAddress = "https://example.com",
                    RouteName = "TestRoute"
                };
            });
        }

        // todo: Move to UI/UX test
        [Fact(Skip = "true")]
        public void WebAddress_WithExceedingMaxLength_ShouldThrowException()
        {
            // Arrange
            string webAddress = new string('a', 201);

            // Act & Assert
            Assert.Throws<ValidationException>(() =>
            {
                var feedEntry = new FeedEntryDataModel
                {
                    Title = "Test Title",
                    WebAddress = webAddress,
                    RouteName = "TestRoute"
                };
            });
        }

        // todo: Move to UI/UX test
        [Fact(Skip = "true")]
        public void RouteName_WithExceedingMaxLength_ShouldThrowException()
        {
            // Arrange
            string routeName = new string('a', 101);

            // Act & Assert
            Assert.Throws<ValidationException>(() =>
            {
                var feedEntry = new FeedEntryDataModel
                {
                    Title = "Test Title",
                    WebAddress = "https://example.com",
                    RouteName = routeName
                };
            });
        }
    }
}
