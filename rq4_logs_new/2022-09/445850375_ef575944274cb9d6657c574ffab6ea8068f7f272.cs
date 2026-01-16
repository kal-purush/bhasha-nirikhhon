using Microsoft.AspNetCore.Components;
using Superkatten.Katministratie.Contract.ApiInterface;
using Superkatten.Katministratie.Contract.Entities;
using Superkatten.Katministratie.Host.Helpers;
using Superkatten.Katministratie.Host.Services;

namespace Superkatten.Katministratie.Host.Pages.SuperkatPages;

public partial class EditSuperkat
{
    [Inject] public Navigation Navigation { get; set; } = null!;
    [Inject] public ISuperkattenListService SuperkattenService { get; set; } = null!;

    [Parameter]
    public Guid? SuperkatId { get; set; }

    private Superkat? _superkat;
    private string _name = string.Empty;

    protected async override Task OnInitializedAsync()
    {
        var superkatten = await SuperkattenService.GetAllSuperkattenAsync();
        _superkat = superkatten
            .Where(s => s.Id == SuperkatId)
            .FirstOrDefault();

        _name = _superkat?.Name ?? string.Empty;
    }

    public void OnCancel()
    {
        Navigation.NavigateBack();
    }

    public async Task OnOk()
    {
        if (_superkat is null)
        {
            return;
        }

        var updateSuperkatParameters = new UpdateSuperkatParameters
        {
            Name = _name
        };

        await SuperkattenService.UpdateSuperkatAsync(_superkat.Id, updateSuperkatParameters);

        Navigation.NavigateBack();
    }
}