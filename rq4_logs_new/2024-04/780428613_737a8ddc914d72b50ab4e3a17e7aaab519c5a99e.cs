using DesktopAssistant.Core.Enums;

namespace DesktopAssistant.Contracts.Services;

/// <summary>
/// APIに関する設定項目のインターフェース
/// </summary>
public interface IApiSetting
{
    /// <summary>
    /// AI生成のサービス名
    /// </summary>
    GenerativeAI GenerativeAI
    {
        get;
    }
    /// <summary>
    /// OpenAIのAPIキー
    /// </summary>
    string OpenAIKey
    {
        get;
    }
    /// <summary>
    /// OpenAIのモデル名
    /// </summary>
    string OpenAIModel
    {
        get;
    }

    /// <summary>
    /// AzureOpenAIのAPIキー
    /// </summary>
    string AzureOpenAIKey
    {
        get;
    }
    /// <summary>
    /// AzureOpenAIのデプロイメント名
    /// </summary>
    string AzureOpenAIModel
    {
        get;
    }
    /// <summary>
    /// AzureOpenAIのエンドポイント
    /// </summary>
    string AzureOpenAIEndpoint
    {
        get;
    }
}