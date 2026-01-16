using Microsoft.AspNetCore.Components;
using Superkatten.Katministratie.Contract.ApiInterface.Reporting;
using Superkatten.Katministratie.Host.Helpers;
using Superkatten.Katministratie.Host.Services.Authentication;
using Superkatten.Katministratie.Host.Services.Interfaces;

namespace Superkatten.Katministratie.Host.Pages.Reports;

public partial class ReportsPage
{
    [Inject] public Navigation Navigation { get; set; } = null!;
    [Inject] public IAuthenticationService AuthenticationService { get; set; } = null!;
    [Inject] public IReportingService ReportingService { get; set; } = null!;

    private void OnBackHome()
    {
        Navigation.NavigateBack();
    }

    private void OnEmailCageCard()
    {
        Navigation.NavigateTo("CageCard");
    }

    private async Task OnCreateWakkerDierInventoryReport()
    {
        var email = AuthenticationService?.User?.Email;
        if (string.IsNullOrEmpty(email))
        {
            //           _notificationString = "Email van ingelogde gebruiker is niet ingevuld. De email kan niet worden verstuurd.";
            //           await _notification.Show();
        }

        var requestParameters = new RequestCatchLocationEmailParameters
        {
            Email = email ?? string.Empty,
            From = DateTime.UtcNow.AddMonths(-3),
            To = DateTime.UtcNow
        };

        await ReportingService.EmailInventoryDetailsReportAsync(requestParameters);
    }

    private async Task OnNotNeutralizedInRefugeReport()
    {
        var email = AuthenticationService?.User?.Email ?? string.Empty;

        await ReportingService.EmailNotNeutralizedInRefugeReportAsync(email);
    }

    private async Task OnNotNeutralizedAdopteesReport()
    {

        var email = AuthenticationService?.User?.Email ?? string.Empty;

        await ReportingService.EmailNotNeutralizedAdopteesReportAsync(email);
    }
}