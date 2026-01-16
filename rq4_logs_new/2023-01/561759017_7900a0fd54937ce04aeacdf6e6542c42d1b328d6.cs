using CuratorMagazineBlazorApp.Data.Services;
using CuratorMagazineWebAPI.Models.Entities.Domains;
using Microsoft.AspNetCore.Components;
using Newtonsoft.Json;

namespace CuratorMagazineBlazorApp.Shared.VDEW
{
    public partial class WorkWithCuratorVDEW
    {
        [Parameter]
        public User? CurrentUser { get; set; }

        [Inject]
        public UserService? UserService { get; set; }

        private List<User>? _users = new();

        void DeleteCurator(User user)
        {

        }

        void AddCurator(User user)
        {

        }

        async Task GetCurator()
        {
            _users = new List<User>();
            var users = await UserService.PostAsync();
            _users = JsonConvert.DeserializeObject<List<User>>(users.Result.Items?.ToString() ?? string.Empty);
        }
    }
}