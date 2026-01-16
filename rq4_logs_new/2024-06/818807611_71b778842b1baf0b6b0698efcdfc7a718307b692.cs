namespace Snailer.GodotCSharp.SettingsManager.Tests.Data;

public class SettingsSetsNullOnHandle : ISettings
{
    public string? StringProperty { get; set; }

    public void InitializeSettings()
    {
    }

    public void HandleSettingChange(string settingName, object value)
    {
        StringProperty = null;
    }
}