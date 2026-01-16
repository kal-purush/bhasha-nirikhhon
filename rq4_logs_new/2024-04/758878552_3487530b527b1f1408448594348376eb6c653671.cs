using Microsoft.AspNetCore.Components;
using System.Linq.Expressions;

namespace Jared.Presentation.Components.Basic;

public partial class InputSelectList
{
    [Parameter]
    public Expression<Func<int>> ValidationFor { get; set; } = default!;
    [Parameter]
    public string? Id { get; set; }
    [Parameter]
    public string? Label { get; set; }
    [Parameter]
    public Dictionary<int, string> Items { get; set; } = new();
    [Parameter]
    public EventCallback<int> ValuePropertyChanged { get; set; }

    protected override bool TryParseValueFromString(string? value, out int result, out string validationErrorMessage)
    {
        result = Items.Single(x => x.Value == value).Key;
        validationErrorMessage = null!;
        return true;
    }
}