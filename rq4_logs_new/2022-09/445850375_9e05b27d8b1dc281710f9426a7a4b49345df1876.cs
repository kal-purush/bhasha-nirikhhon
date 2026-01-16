using Microsoft.AspNetCore.Components;
using Superkatten.Katministratie.Host.Helpers;
using Superkatten.Katministratie.Host.Services.Authentication;

namespace Superkatten.Katministratie.Host.Pages.Users;

public partial class Register
{
    private string _error = string.Empty;
    private string _name = string.Empty;
    private string _userName = string.Empty;
    private string _email = string.Empty;
    private string _password = string.Empty;

    [Inject] public IAuthenticationService AuthenticationService { get; set; } = null!;
    [Inject] public Navigation Navigation { get; set; } = null!;

    private async void HandleValidSubmit()
    {
        try
        {
            await AuthenticationService.RegisterAsync(_userName, _password, _name, _email);

            Navigation.NavigateBack();
        }
        catch (Exception ex)
        {
            _error = ex.Message;
            StateHasChanged();
        }
    }

    private void OnBackHome()
    {
        Navigation.NavigateBack();
    }
}