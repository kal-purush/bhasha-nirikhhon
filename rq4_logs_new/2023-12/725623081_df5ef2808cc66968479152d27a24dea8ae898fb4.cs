using FrontendBlazorWebAssembly.Services;
using Microsoft.AspNetCore.Components;
using Shared;

namespace FrontendBlazorWebAssembly.Pages
{
    public class RegistrationPageBase : ComponentBase
    {
        public User User { get; set; } = new User();


        public string errorMessage;

        [Inject] public IRegisterUserService RegisterService { get; set; }
        [Inject] public NavigationManager NavigationManager { get; set; }




        public async Task RegisterUserHandler()
        {
            try
            {

                // Call the RegisterUser service with the user object
                User registeredUser = await RegisterService.RegisterUser(User);

                if (registeredUser != null)
                {
                    // Registration successful, navigate to another URL
                    NavigationManager.NavigateTo("/login"); // Change "/successPage" to the desired URL
                }
                else
                {
                    // Registration failed
                    errorMessage = "Registration failed. Please try again."; // Set your error message
                }
            }
            catch (Exception ex)
            {
                // Handle other exceptions if needed
                errorMessage = $"An error occurred: {ex.Message}";
            }
        }
    }
}