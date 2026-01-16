using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using AutoMapper;
using MediatR;
using Moq;

using QuizSite.Contracts.Database;
using QuizSite.Domain.Commands;
using QuizSite.Domain.Database;
using QuizSite.Domain.Queries;
using QuizSite.Domain.Services;
using QuizSite.Domain.Services.Interfaces;
using Shouldly;

namespace QuizSite.UnitTests.Queries;

public class SubmitQuizResultCommandTest
{
    private readonly Mock<IQuizSiteService> _quizService;
    private readonly IRequestHandler<SubmitQuizResultCommand, SubmitQuizResultCommandResult> _handler;

    public SubmitQuizResultCommandTest()
    {
        _quizService = new Mock<IQuizSiteService>();
        _handler = new SubmitQuizResultCommandHandler(_quizService.Object);        
    }

    [Fact]
    public async Task QuizQuestionsShouldBeFound()
    {
        //Arrange       

        _quizService.Setup(x => x.insertUserResultIntoDb(It.IsAny<Result>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(new Random().Next(10, 100)));

        var command = new SubmitQuizResultCommand
        {
            Category = Guid.NewGuid().ToString(),
            Score = new Random().Next(),
            Username = Guid.NewGuid().ToString(),
        };

        //Act
        var result = await _handler.Handle(command, CancellationToken.None);

        //Assert
        result.ShouldNotBeNull();
        result.Id.ShouldBeInRange(10, 100);
    }
}