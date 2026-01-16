using LinkBakery.Web.Cms.Services.Interfaces;
using LinkBakery.Web.Cms.Dtos;
using Microsoft.AspNetCore.Components;

namespace LinkBakery.Web.Cms.Components
{
    public class TrackingLinkEditComponentBase : ComponentBase
    {
        [Inject]
        private ITrackingLinkService _trackingLinkService { get; set; }
        [Inject]
        private NavigationManager _uriHelper { get; set; }



        [Parameter]
        public int Id { get; set; }


        protected TrackingLinkEditDto trackingLink;

        protected override async Task OnInitializedAsync()
            => trackingLink = await _trackingLinkService.FindByIdAsync(Id);


        protected void HandleValidSubmit()
        {
            _trackingLinkService.UpdateEntry(trackingLink);
            _uriHelper.NavigateTo("/TrackingLinks/Overview", true);
        }
    }
}