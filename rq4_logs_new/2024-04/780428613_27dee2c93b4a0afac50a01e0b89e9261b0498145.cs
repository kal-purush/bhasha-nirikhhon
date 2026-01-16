
using Microsoft.UI.Xaml.Markup;

namespace DesktopAssistant.Helpers;

// ライブラリのバグのため、Microsoft.UI.XamlのEnumではない場合は、ViewModelにCommandParameterで渡したときにint型として受け付けることしかできなくなっている
// https://github.com/CommunityToolkit/dotnet/discussions/407
// https://github.com/microsoft/microsoft-ui-xaml/issues/7633

[MarkupExtensionReturnType(ReturnType = typeof(object))]
public class EnumValueExtension : MarkupExtension
{
    public Type? Type
    {
        get; set;
    }

    public string? Member
    {
        get; set;
    }

    protected override object ProvideValue()
    {
        if (Type is null || Member is null)
        {
            return null!;
        }

        return Enum.Parse(Type, Member);
    }
}