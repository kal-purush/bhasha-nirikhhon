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
using QuizSite.Domain.Database;
using QuizSite.Domain.Queries;
using QuizSite.Domain.Services;
using QuizSite.Domain.Services.Interfaces;
using Shouldly;

namespace QuizSite.UnitTests.Queries;

public class GetQuizQuestionsQueryTest
{
    private readonly Mock<IQuizSiteService> _quizService;
    private readonly IRequestHandler<GetQuizQuestionsQuery, GetQuizQuestionsQueryResult> _handler;
    private readonly IMapper _mapper;

    public GetQuizQuestionsQueryTest()
    {
        _quizService = new Mock<IQuizSiteService>();
        _mapper = CreateMapperForTest();
        _handler = new GetQuizQuestionsQueryHandler(_mapper, _quizService.Object);        
    }

    [Fact]
    public async Task QuizQuestionsShouldBeFound()
    {
        //Arrange
        var fixture = new Fixture();
        fixture.Behaviors.OfType<ThrowingRecursionBehavior>().ToList()
            .ForEach(b => fixture.Behaviors.Remove(b));
        fixture.Behaviors.Add(new OmitOnRecursionBehavior());

        var listOfQuestions = fixture.CreateMany<Question>(5).ToList();

        _quizService.Setup(x => x.getQuizQuestionsByCategory(It.IsAny<GetQuizQuestionsQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(listOfQuestions));

        var query = new GetQuizQuestionsQuery
        {
            Category = listOfQuestions[0].QuizCategory
        };

        //Act
        var result = await _handler.Handle(query, CancellationToken.None);

        //Assert
        result.ShouldNotBeNull();
    }

    private IMapper CreateMapperForTest()
    {
        var mappingConfig = new MapperConfiguration(mc => {
            mc.AddProfile(new AppMappingProfile());
        });
        IMapper mapper = mappingConfig.CreateMapper();
        return mapper;
    }
}