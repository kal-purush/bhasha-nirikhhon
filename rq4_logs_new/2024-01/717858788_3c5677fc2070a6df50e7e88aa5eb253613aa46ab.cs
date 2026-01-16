using MediatR;
using Entity = QuizzenApp.Domain.Entities.AnswerAggregate;
using QuizzerApp.Application.Common.Interfaces;

namespace QuizzerApp.Application.Features.Commands.Answer.CreateAnswer;

public class CreateAnswerCommandHandler : IRequestHandler<CreateAnswerCommand, bool>
{
    private readonly IRepositoryManager _manager;

    public CreateAnswerCommandHandler(IRepositoryManager manager) => _manager = manager;

    public async Task<bool> Handle(CreateAnswerCommand request, CancellationToken cancellationToken)
    {
        Entity.Answer answer = new(id: Guid.NewGuid(),
                                    text: request.text,
                                    userId: request.userId,
                                    questionId: request.questionId);

        //TODO: save answer here.

        return true;
    }
}