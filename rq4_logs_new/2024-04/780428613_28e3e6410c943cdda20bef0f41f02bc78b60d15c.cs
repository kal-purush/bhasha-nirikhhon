using DesktopAssistant.Core.Enums;

namespace DesktopAssistant.Contracts.Services;

/// <summary>
/// API設定を提供
/// </summary>
public interface IApiSettingService
{
    GenerativeAI GenerativeAI
    {
        get;
    }

    Task InitializeAsync();

    /// <summary>
    /// 生成AIを変更する
    /// </summary>
    /// <param name="generativeAI"></param>
    /// <returns></returns>
    Task SetGenerativeAIAsync(GenerativeAI theme);

    /// <summary>
    /// 設定内容をアプリに反映する
    /// </summary>
    /// <returns></returns>
    Task SetRequestedSettingAsync();
}