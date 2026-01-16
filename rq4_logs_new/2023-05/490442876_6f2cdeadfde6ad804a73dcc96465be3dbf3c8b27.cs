using GiftSuggester.Core.CommonClasses;
using GiftSuggester.Core.Users.Models;
using GiftSuggester.Core.Users.Repositories;

namespace GiftSuggester.Core.Users.Services.Implementations;

public class UserService: IUserService
{
    private readonly IUnitOfWork _unitOfWork;
    private readonly IUserRepository _userRepository;

    public UserService(IUnitOfWork unitOfWork, IUserRepository userRepository)
    {
        _unitOfWork = unitOfWork;
        _userRepository = userRepository;
    }

    public async Task AddAsync(UserCreationModel creationModel, CancellationToken cancellationToken)
    {
        var user = new User
        {
            Id = Guid.NewGuid(),
            Name = creationModel.Name,
            Login = creationModel.Login,
            Email = creationModel.Email,
            Password = creationModel.Password,
            DateOfBirth = creationModel.DateOfBirth,
            GroupIds = new List<Guid>()
        };
        
        await _userRepository.AddAsync(user, cancellationToken);

        await _unitOfWork.SaveChangesAsync(cancellationToken);
    }

    public Task<User> GetByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        return _userRepository.GetByIdAsync(id, cancellationToken);
    }

    public async Task UpdateAsync(User user, CancellationToken cancellationToken)
    {
        await _userRepository.AddAsync(user, cancellationToken);

        await _unitOfWork.SaveChangesAsync(cancellationToken);
    }

    public async Task RemoveByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        await _userRepository.RemoveByIdAsync(id, cancellationToken);

        await _unitOfWork.SaveChangesAsync(cancellationToken);
    }
}