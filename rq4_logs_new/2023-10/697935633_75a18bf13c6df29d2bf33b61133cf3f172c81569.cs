using vabalas_api.Controllers.Auth.Dtos;
using vabalas_api.Models;
using vabalas_api.Repositories;

namespace vabalas_api.Service;

public class UserService : IUserService
{
    private readonly IUserRepository _userRepository;

    public UserService(IUserRepository userRepository, IJwtService jwtService)
    {
        _userRepository = userRepository;
    }

    public async Task<User> Register(UserRegisterDto userDto)
    {
        var passwordHash = BCrypt.Net.BCrypt.HashPassword(userDto.Password);
        var user = new User
        {
            Firstname = userDto.Firstname,
            Lastname = userDto.Lastname,
            Email = userDto.Email,
            PasswordHash = passwordHash,
            createdAt = DateTime.UtcNow,
            updatedAt = DateTime.UtcNow
        };

        return await _userRepository.Add(user);
    }

    public async Task<User> GetByEmail(string email)
    {
        return await _userRepository.GetByEmail(email);
    }
}