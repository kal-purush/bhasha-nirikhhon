using Microsoft.EntityFrameworkCore;
using ArtVault.API.Controllers;
using ArtVault.API.Data;
using ArtVault.API.Models;
using AutoMapper;
using ArtVault.API.DTOs;
using Microsoft.AspNetCore.Mvc;
using ArtVault.API.Profiles;

namespace ArtVault.API.Tests.Controllers
{
    public class CommentControllerTests
    {
        private readonly ApplicationDBContext _dbContext;
        private readonly IMapper _mockMapper;
        private readonly CommentController _controller;
        public CommentControllerTests()
        {
            _dbContext = GetInMemoryDbContext();
            _mockMapper = GetMockMapper();
            _controller = new CommentController(_dbContext, _mockMapper);
        }
        private ApplicationDBContext GetInMemoryDbContext()
        {
            var options = new DbContextOptionsBuilder<ApplicationDBContext>()
                .UseInMemoryDatabase(Guid.NewGuid().ToString())
                .Options;

            var dbContext = new ApplicationDBContext(options);
            return dbContext;
        }

        private IMapper GetMockMapper()
        {
            var config = new MapperConfiguration(cfg =>
            {
                cfg.AddProfile<MappingProfile>();
            });

            return config.CreateMapper();
        }

        [Fact]
        public async Task GetComments_ReturnsCommentsWhenExists()
        {
            var comment1 = new Comment
            {
                Id = Guid.NewGuid(),
                Content = "Test Comment 1",
                UserId = Guid.NewGuid(),
                Username = "User1",
                PostId = Guid.NewGuid()
            };

            var comment2 = new Comment
            {
                Id = Guid.NewGuid(),
                Content = "Test Comment 2",
                UserId = Guid.NewGuid(),
                Username = "User2",
                PostId = Guid.NewGuid()
            };

            _dbContext.Comments.Add(comment1);
            _dbContext.Comments.Add(comment2);
            _dbContext.SaveChanges();
   

            var result = await _controller.GetComments();

            var okResult = Assert.IsType<OkObjectResult>(result);
            var commentDtos = Assert.IsType<List<CommentDto>>(okResult.Value);

            Assert.Equal(2, commentDtos.Count);
        }

        [Fact]
        public async Task GetComments_ReturnsEmptyWhenDoesNotExist()
        {
            _dbContext.Comments.RemoveRange(_dbContext.Comments);
            _dbContext.SaveChanges();

            var result = await _controller.GetComments();

            var okResult = Assert.IsType<OkObjectResult>(result);
            var commentDtos = Assert.IsType<List<CommentDto>>(okResult.Value);

            Assert.Empty(commentDtos);
        }

        [Fact]
        public async Task GetComment_ReturnsWhenCommentExists()
        {
            var comment3 = new Comment
            {
                Id = Guid.NewGuid(),
                Content = "Test Comment 1",
                UserId = Guid.NewGuid(),
                Username = "User1",
                PostId = Guid.NewGuid()
            };

            _dbContext.Comments.Add(comment3);
            _dbContext.SaveChanges();

            var result = await _controller.GetComment(comment3.Id);

            var okResult = Assert.IsType<OkObjectResult>(result);

            var commentDto = Assert.IsType<CommentDto>(okResult.Value);

            Assert.Equal(comment3.Id, commentDto.Id);
            Assert.Equal(comment3.Content, commentDto.Content);
        }

        [Fact]
        public async Task GetComment_ReturnsNotFoundsWhenDoesNotExist()
        {
            var nonExistingCommentId = Guid.NewGuid();

            var result = await _controller.GetComment(nonExistingCommentId);

            Assert.IsType<NotFoundResult>(result);
        }

        [Fact]
        public async Task CreateComment_CreatesCommentWhenModelIsValid()
        {
            var createCommentDto = new CreateCommentDto
            {
                Content = "I love the theme to this piece!",
                UserId = Guid.NewGuid(),
                Username = "TestUsername",
                PostId = Guid.NewGuid()
            };
             
            var result = await _controller.CreateComment(createCommentDto);

            var createdAtActionResult = Assert.IsType<CreatedAtActionResult>(result);
            var returnValue = Assert.IsType<CommentDto>(createdAtActionResult.Value); 
            Assert.Equal(createCommentDto.Content, returnValue.Content);
            Assert.Equal(createCommentDto.Username, returnValue.Username);
            Assert.Equal(createCommentDto.PostId, returnValue.PostId); 
        }

        [Fact]
        public async Task UpdateComment_UpdatesCommentWhenModelIsValid()
        {
            var comment = new Comment
            {
                Id = Guid.NewGuid(),
                Content = "Old content",
                UserId = Guid.NewGuid(),
                Username = "User1",
                PostId = Guid.NewGuid()
            };
            _dbContext.Comments.Add(comment);
            await _dbContext.SaveChangesAsync();

            var updateCommentDto = new UpdateCommentDto
            {
                Content = "Updated content"
            };

            var result = await _controller.UpdateComment(comment.Id, updateCommentDto);

            Assert.IsType<NoContentResult>(result);

            var updatedComment = await _dbContext.Comments.FindAsync(comment.Id);
            Assert.Equal("Updated content", updatedComment?.Content);
        }

        [Fact]
        public async Task DeleteComment_DeletesCommentWhenCommentExists()
        {
            var commentId = Guid.NewGuid();
            var comment = new Comment
            {
                Id = commentId,
                Content = "Test Comment",
                UserId = Guid.NewGuid(),
                Username = "User1",
                PostId = Guid.NewGuid()
            };
            _dbContext.Comments.Add(comment);
            await _dbContext.SaveChangesAsync();

            var result = await _controller.DeleteComment(commentId);

            Assert.IsType<NoContentResult>(result);

            var deletedComment = await _dbContext.Comments.FindAsync(commentId);
            Assert.Null(deletedComment); 
        }

        [Fact]
        public async Task DeleteComment_ReturnsNotFoundCommentDoesNotExist()
        {
            var nonExistentCommentId = Guid.NewGuid();
            var result = await _controller.DeleteComment(nonExistentCommentId);
            Assert.IsType<NotFoundResult>(result);
        }
    }
}