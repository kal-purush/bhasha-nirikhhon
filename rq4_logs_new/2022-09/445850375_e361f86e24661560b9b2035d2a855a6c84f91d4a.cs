using Superkatten.Katministratie.Host.Entities;
using Superkatten.Katministratie.Host.LocalStorage;

namespace Superkatten.Katministratie.Host.Services.Authentication;

public class UserLoginService : IUserLoginService
{
    private const string STORAGE_USER_KEY = "user";

    private readonly ILocalStorageService _localStorageService;

    public User? User { get; private set; }

    public UserLoginService(ILocalStorageService localStorageService)
    {
        _localStorageService = localStorageService;
    }

    public bool IsAuthenticated => User is not null;

    public async Task InitializeAsync()
    {
        User = await _localStorageService.GetItemAsync<User>(STORAGE_USER_KEY);
    }

    public Task ResetAsync()
    {
        User = null!;

        return _localStorageService.RemoveItemAsync(STORAGE_USER_KEY);
    }

    public async Task SetUserAsync(User? user)
    {
        if (user is null)
        {
            await ResetAsync();
        }

        User = user;
        await _localStorageService.SetItemAsync(STORAGE_USER_KEY, user);
    }
}