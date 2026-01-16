using Microsoft.AspNetCore.Components;
using System.Linq.Expressions;

namespace Jared.Presentation.Components.Basic;

public partial class InputTimeSpanField
{
    [Parameter]
    public Expression<Func<TimeSpan>> ValidationFor { get; set; } = default!;
    [Parameter]
    public string? Id { get; set; }
    [Parameter]
    public string? Label { get; set; }

    protected override bool TryParseValueFromString(string? value, out TimeSpan result, out string validationErrorMessage)
    {
        result = TimeSpan.Parse(value!);
        validationErrorMessage = null!;
        return true;
    }
}