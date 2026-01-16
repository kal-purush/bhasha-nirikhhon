using CuratorMagazineBlazorApp.Data.Services;
using CuratorMagazineWebAPI.Models.Entities;
using CuratorMagazineWebAPI.Models.Entities.Domains;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Newtonsoft.Json;

namespace CuratorMagazineBlazorApp.Shared
{
    public partial class WorkWithVDEW
    {
        [Parameter]
        public User? CurrentUser { get; set; }

        [Inject]
        public UserService? UserService { get; set; }

        private List<User>? _users = new();

        void DeleteVDEW(User user)
        {

        }

        void AddVDEW(User user)
        {

        }

        async Task GetVDEW()
        {
            _users = new List<User>();
            var users = await UserService.PostAsync();
            _users = JsonConvert.DeserializeObject<List<User>>(users.Result.Items?.ToString() ?? string.Empty);
        }

        protected override async Task OnInitializedAsync()
        {
            await Task.Delay(1);
            await GetVDEW();
        }
    }
}