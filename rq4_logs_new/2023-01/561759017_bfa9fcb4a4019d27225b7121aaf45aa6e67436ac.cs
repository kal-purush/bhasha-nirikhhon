using CuratorMagazineBlazorApp.Data.Services;
using CuratorMagazineWebAPI.Models.Entities.Domains;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Newtonsoft.Json;


namespace CuratorMagazineBlazorApp.Shared.ModalWindows
{
    public partial class ModalChangeVDEWWindow
    {

        [Parameter]
        public User? _vdew { get; set; }

        private List<Division>? _divisions = new();


        [Parameter]
        public EventCallback<User> RoleCallback { get; set; }

        /// <summary>
        /// Called when [finish].
        /// </summary>
        /// <param name="editContext">The edit context.</param>
        private void OnFinish(EditContext editContext)
        {
            Console.WriteLine($"Success: {JsonConvert.SerializeObject(_vdew)}");
        }

        /// <summary>
        /// Called when [finish failed].
        /// </summary>
        /// <param name="editContext">The edit context.</param>
        private void OnFinishFailed(EditContext editContext)
        {
            Console.WriteLine($"Failed: {JsonConvert.SerializeObject(_vdew)}");

        }

        public void ChangeVDEW()
        {

        }
    }
}