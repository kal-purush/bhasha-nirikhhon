namespace Snailer.GodotCSharp.SettingsManager;

/// <summary>
/// Represents a settings class to be managed by <see cref="SettingsManager{T}"/>
/// </summary>
public interface ISettings
{
    /// <summary>
    /// Called after settings have been initialized, game parameters should be set based on setting values.
    /// </summary>
    public void InitializeSettings();

    /// <summary>
    /// Called when a setting is changed, new game parameters should be set.
    /// </summary>
    /// <param name="settingName">The setting that was changed.</param>
    /// <param name="value">The new value of the setting.</param>
    public void HandleSettingChange(string settingName, object value);
}