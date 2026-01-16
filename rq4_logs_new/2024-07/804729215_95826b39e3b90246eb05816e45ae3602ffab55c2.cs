using Application.Abstractions.Data;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.PaidSalary.Deletes;
using Domain.Abstractions.Exceptions;

namespace Application.UserCases.Commands.PaidSalaries.Deletes;

internal sealed class DeletePaidSalaryCommandHandler
    (IPaidSalaryRepository _paidSalaryRepository,
    IUserRepository _userRepository,
    IUnitOfWork _unitOfWork
    ) : ICommandHandler<DeletePaidSalaryCommand>
{
    public async Task<Result.Success> Handle(DeletePaidSalaryCommand request, CancellationToken cancellationToken)
    {
        var isPaidSalaryExist = await _paidSalaryRepository.IsPaidSalaryExistsAsync(request.Id);
        if (!isPaidSalaryExist)
        {
            throw new MyValidationException("Lương đã thanh toán không tồn tại.");
        }
        var paidSalary = await _paidSalaryRepository.GetPaidSalaryById(request.Id);
        var userId = paidSalary.UserId;
        var user = await _userRepository.GetUserByIdAsync(userId);
        var accountBalanceCurrent = user?.AccountBalance ?? 0;
        var AccountBalanceUpdate = accountBalanceCurrent + paidSalary.Salary;
        user.UpdateAccountBalance(AccountBalanceUpdate);
        _userRepository.Update(user);
        _paidSalaryRepository.DeletePaidSalary(paidSalary);
        _userRepository.Update(user);
        await _unitOfWork.SaveChangesAsync();

        return Result.Success.Delete();
    }
}