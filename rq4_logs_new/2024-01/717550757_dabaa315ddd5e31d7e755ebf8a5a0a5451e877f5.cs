using System.Data;
using MediatR;
using SimpleLibraryApp.Core.Aggregates.AuthAggregates;
using SimpleLibraryApp.Core.Response;

namespace SimpleLibraryApp.Service.PersonelInfo.Commands.ChangeEmailByUserId;

public class Handler : IRequestHandler<Command, BaseResponse>
{
    private readonly IAuthRepository _authRepository;
    private readonly IDbTransaction _transaction;

    public Handler(IAuthRepository authRepository, IDbTransaction transaction)
    {
        _authRepository = authRepository;
        _transaction = transaction;
    }

    public async Task<BaseResponse> Handle(Command request, CancellationToken cancellationToken)
    {
        var userToUpdate = await _authRepository.GetByIdAsync(request.UserId);

        if (userToUpdate is null)
        {
            _transaction.Rollback();
            return ResponseFactory.FailResponse("Kullanıcı bulunamadı");
        }

        if (userToUpdate.AccountEndDate is not null)
        {
            _transaction.Rollback();
            return ResponseFactory.FailResponse("Kullanıcı hesabı kapalı olduğundan güncelleme yapılamaz");
        }

        var isEmailExists = await _authRepository.GetUserByEmailAsync(request.NewEmail);

        if (isEmailExists is not null)
        {
            _transaction.Rollback();
            return ResponseFactory.FailResponse("Değiştirmek istediğiniz e-posta adresi sistemde zaten mevcut");
        }

        var result = await _authRepository.ChangeEmailByUserIdAsync(request.UserId, request.NewEmail);

        if (result == 0)
        {
            _transaction.Rollback();
            return ResponseFactory.FailResponse("Güncelleme işlemi başarısız");
        }

        _transaction.Commit();
        return ResponseFactory.SuccessResponse("Güncelleme işlemi başarılı");
    }
}