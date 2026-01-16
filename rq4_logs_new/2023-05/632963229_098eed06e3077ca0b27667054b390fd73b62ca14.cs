using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Moq;
using MovieFiles.Api.Functions;
using MovieFiles.Core.Interfaces;
using MovieFiles.Core.Models;
using Newtonsoft.Json;
using System.Text;

namespace MovieFiles.Test
{
    public class CommentsFunctionTest
    {
        private readonly Mock<ILogger<Comments>> _loggerMock;
        private readonly Mock<ICommentService> _commentServiceMock;
        private readonly Comments _commentsFunction;

        public CommentsFunctionTest()
        {
            _loggerMock = new Mock<ILogger<Comments>>();
            _commentServiceMock = new Mock<ICommentService>();
            _commentsFunction = new Comments(_loggerMock.Object, _commentServiceMock.Object);
        }
        [Fact]
        public async Task GetComments_ReturnsOkObjectResult_WhenCommentsExist()
        {
            // Arrange
            var mockHttpRequest = CreateMockHttpRequest("page", "1");
            var mockCommentService = new Mock<ICommentService>();
            var movieId = "1";
            var comments = GenerateCommentsList(5);
            // Setup for GetComments method
            mockCommentService
                .Setup(service => service.GetComments(It.IsAny<int>(), It.IsAny<int>()))
                .ReturnsAsync(comments);

            // Create new instance of Comments function with mocked comment service
            var commentsFunction = new Comments(_loggerMock.Object, mockCommentService.Object);

            // Act
            var response = await commentsFunction.GetComments(mockHttpRequest, movieId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(response);
            var returnValue = Assert.IsType<List<Comment>>(okResult.Value);

            Assert.Equal(comments, returnValue);
        }


        [Fact]
        public async Task Comment_ReturnsOkResult_WhenCommentIsPostedSuccessfully()
        {
            // Arrange
            var movieId = "1";
            var userId = Guid.NewGuid().ToString();
            var commentText = "This is a test comment";
            var commentData = new Comment { Author = userId, Text = commentText };

            _commentServiceMock.Setup(x => x.Comment(It.IsAny<Comment>(), It.IsAny<int>(), It.IsAny<Guid>())).Returns(Task.CompletedTask);

            var mockHttpRequest = CreateMockHttpRequest(commentData);

            // Act
            var response = await _commentsFunction.Comment(mockHttpRequest, movieId, userId);

            // Assert
            Assert.IsType<OkResult>(response);
        }

        private HttpRequest CreateMockHttpRequest(string queryKey, string queryValue)
        {
            var context = new DefaultHttpContext();
            var request = context.Request;
            request.Query = new QueryCollection(new Dictionary<string, StringValues> { { queryKey, queryValue } });

            return request;
        }

        private HttpRequest CreateMockHttpRequest(Comment comment)
        {
            var context = new DefaultHttpContext();
            var request = context.Request;
            request.Body = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(comment)));

            return request;
        }

        private List<Comment> GenerateCommentsList(int numberOfComments)
        {
            var mockCommentsList = new List<Comment>();

            for (int i = 1; i <= numberOfComments; i++)
            {
                mockCommentsList.Add(new Comment() { Author = Guid.NewGuid().ToString(),  Text = $"Comment{i}" });
            }

            return mockCommentsList;
        }
    }
}