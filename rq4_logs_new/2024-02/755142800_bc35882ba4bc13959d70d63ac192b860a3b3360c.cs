using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Application.IRepositories;
using Application.Models.Dtos;
using AutoMapper;
using Domain.Entities;
using Infrastructure.Services;
using Moq;

namespace UnitTests;

public class QuestionServiceTests
{
    private readonly Mock<IQuestionRepository> _mockQuestionRepository;
    
    private readonly Mock<IOptionRepository> _mockOptionRepository;
    
    private readonly Mock<IMapper> _mockMapper;
    
    private readonly QuestionService _questionService;

    public QuestionServiceTests()
    {
        _mockQuestionRepository = new Mock<IQuestionRepository>();
        _mockOptionRepository = new Mock<IOptionRepository>();
        _mockMapper = new Mock<IMapper>();

        _questionService = new QuestionService(_mockQuestionRepository.Object, _mockMapper.Object, _mockOptionRepository.Object);
    }

    [Fact]
    public async Task GetQuestionsWithOptionsByTestAsync_ReturnsCorrectData()
    {
        // Arrange
        var testId = Guid.NewGuid();
        var pageNumber = 1;
        var pageSize = 10;
        var cancellationToken = new CancellationToken();

        var questions = new List<Question>
        {
            new Question { Id = Guid.NewGuid(), TestId = testId, Title = "Question 1" }
        };

        var options = new List<Option>
        {
            new Option { Id = Guid.NewGuid(), QuestionId = questions[0].Id, Text = "Option 1", IsCorrect = true }
        };

        _mockQuestionRepository.Setup(x => x.GetPageAsync(pageNumber, pageSize, It.IsAny<Expression<Func<Question, bool>>>(), cancellationToken))
            .ReturnsAsync(questions);

        _mockOptionRepository.Setup(x => x.GetPageAsync(pageNumber, pageSize, It.IsAny<Expression<Func<Option, bool>>>(), cancellationToken))
            .ReturnsAsync(options);

        _mockMapper.Setup(m => m.Map<List<QuestionDto>>(It.IsAny<List<Question>>()))
            .Returns(new List<QuestionDto> { new QuestionDto { Id = questions[0].Id, Title = questions[0].Title } });

        _mockMapper.Setup(m => m.Map<List<OptionDto>>(It.IsAny<List<Option>>()))
            .Returns(new List<OptionDto> { new OptionDto { Id = options[0].Id, Text = options[0].Text, IsCorrect = options[0].IsCorrect } });

        // Act
        var result = await _questionService.GetQuestionsWithOptionsByTestAsync(testId, pageNumber, pageSize, cancellationToken);

        // Assert
        Assert.Single(result);
        var questionOptionDto = result[0];
        Assert.Equal(questions[0].Id, questionOptionDto.QuestionDto.Id);
        Assert.Single(questionOptionDto.OptionDtos);
        var optionDto = questionOptionDto.OptionDtos[0];
        Assert.Equal(options[0].Id, optionDto.Id);
        Assert.Equal(options[0].Text, optionDto.Text);
        Assert.True(optionDto.IsCorrect);
    }
}