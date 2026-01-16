using Application.Abstractions.Data;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.SalaryHistory.Creates;
using FluentValidation;

namespace Application.UserCases.Commands.SalaryHistories.Creates;

public sealed class CreateSalaryHistoryCommandHandler
    (ISalaryHistoryRepository _salaryHistoryRepository,
    IValidator<CreateSalaryHistoryRequest> _validator,
    IUnitOfWork _unitOfWork) : ICommandHandler<CreateSalaryHistoryCommand>
{
    public Task<Result.Success> Handle(CreateSalaryHistoryCommand request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}