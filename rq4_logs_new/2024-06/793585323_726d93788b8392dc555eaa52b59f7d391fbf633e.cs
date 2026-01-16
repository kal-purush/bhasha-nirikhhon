using Microsoft.AspNetCore.Components;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Presentation.Components
{
    public partial class ConfirmPopUp
    {
        [Parameter]
        public bool ConfirmBtnEnabled { get; set; } = true;
        [Parameter]
        public bool CancelBtnEnabled { get; set; } = true;
        [Parameter]
        public string ConfrimBtnText { get; set; } = "Confirm";
        [Parameter]
        public string CancelBtnText { get; set; } = "Cancel";

		[Parameter]
        public string Title { get; set; } = "Confirm";

        [Parameter]
        public string Message { get; set; } = "Are you sure?";

        [Parameter]
        public EventCallback<bool> OnClose { get; set; }

        private bool Show { get; set; }

        public void ShowDialog()
        {
            Show = true;
            StateHasChanged();
        }

        private async Task Confirm()
        {
            Show = false;
            await OnClose.InvokeAsync(true);
        }

        private async Task Close()
        {
            Show = false;
            await OnClose.InvokeAsync(false);
        }
    }
}