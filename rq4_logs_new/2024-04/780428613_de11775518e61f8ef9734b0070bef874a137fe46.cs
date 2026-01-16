using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DesktopAssistant.Core.Contracts.Services;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.ChatCompletion;
using Microsoft.SemanticKernel.Connectors.OpenAI;

namespace DesktopAssistant.Core.Services;
// 呼び方
// ・カーネルを作る
// ・InvokePromptAsyncでプロンプトを入れるか、関数を作ってInvokeAsyncで実行する
// ・ChatHistoryを作って、AddUserMessage, AddAssistantMessageで入出力を追加する、IChatCompletionService で生成する。

/// <summary>
/// とりあえずSemanticKernelをラッピングする
/// </summary>
public class SemanticService : ISemanticService
{
    public async Task<string> TestAsync(bool isAzure)
    {
        // カーネルのセットアップ
        var kernel = Setup(isAzure);

        // プロンプトを作成
        var prompt = """Hello, world! と表示する C# のプログラムを書いてください。""";  // 3つのダブルクォーテーション符号は複数行にわたる文字列リテラルを定義するのに使用する
        var result = await kernel.InvokePromptAsync(prompt);
        return result.GetValue<string>();
    }


    /// <summary>
    /// APIキーからKernelを作成する
    /// </summary>
    /// <param name="isAzure">Azureならtrue, OpenAIならfalse</param>
    /// <returns></returns>
    private static Kernel Setup(bool isAzure)
    {
        var builder = Kernel.CreateBuilder();
        if (isAzure)
        {
            builder.AddAzureOpenAIChatCompletion(
                     "gpt-35-turbo",                      // Azure OpenAI Deployment Name
                     "https://contoso.openai.azure.com/", // Azure OpenAI Endpoint
                     "...your Azure OpenAI Key...");      // Azure OpenAI Key
        }
        else
        {
            // OpenAIの場合
            builder.AddOpenAIChatCompletion(
                "gpt-3.5-turbo",
                "<< ここに OpenAI の API Key を記載する >>");
        }
        return builder.Build();
    }

    #region チャットを作る例
    private static async Task<string> ChatTestAsync()
    {
        var kernel = Setup(true);

        // Chat の履歴を作成
        var chatHistory = new ChatHistory();
        chatHistory.AddSystemMessage("""
        ソースコードジェネレーターとして振舞ってください。
        ユーザーが指示する内容の C# のソースコードを生成してください。
        生成結果には C# のコード以外を含まないようにしてください。
        """);

        // 入出力例を追加しておく
        chatHistory.AddUserMessage("Hello, world! と表示するプログラムを書いてください。");
        chatHistory.AddAssistantMessage("""
        using System;

        class Program
        {
            static void Main()
            {
                Console.WriteLine("Hello, world!");
            }
        }
        """);
        chatHistory.AddUserMessage("10 * 300 の結果を表示するプログラムを書いてください。");
        chatHistory.AddAssistantMessage("""
        using System;

        class Program
        {
            static void Main()
            {
                Console.WriteLine($"10 * 300 = {10 * 300}");
            }
        }
        """);

        // 本番
        chatHistory.AddUserMessage("与えられた数字が素数かどうか判定するプログラムを書いてください。");

        // IChatCompletionService を使って Chat Completion API を呼び出す
        var chatService = kernel.GetRequiredService<IChatCompletionService>();
        var response = await chatService.GetChatMessageContentAsync(chatHistory);
        if (response.Items.FirstOrDefault() is TextContent responseText)
        {
            return responseText.Text;
        }
        return "No response";
    }
    #endregion

    #region 関数を作る例
    // 例
    //        // テキストを要約する（これは熱力学の例）
    //        var text1 = """
    //熱力学の第1法則 - エネルギーは創造も破壊もできない。
    //熱力学第2法則 - 自然発生的な過程では、宇宙のエントロピーは増大する。
    //熱力学第3法則 - ゼロケルビンの完全な結晶はエントロピーがゼロである。
    //""";

    //var result = await GetSummaryExample(kernel, summarize, text1);
    // Output:
    //   Energy conserved, entropy increases, zero entropy at 0K.

    /// <summary>
    /// 要約を取得する例
    /// </summary>
    /// <param name="kernel"></param>
    /// <param name="summarize">KernelFunction</param>
    /// <param name="text"></param>
    /// <returns></returns>
    private static async Task<FunctionResult> GetSummaryExample(Kernel kernel, KernelFunction summarize, string text) => await kernel.InvokeAsync(summarize, new() { ["input"] = text });

    /// <summary>
    /// プロンプトから関数を作成する例
    /// </summary>
    /// <param name="kernel"></param>
    /// <returns>テキストを要約する関数</returns>
    private static KernelFunction CreateFunctionExample(Kernel kernel)
    {
        var prompt = """
        {{$input}}
        
        One line TLDR with the fewest words.
        """;

        return kernel.CreateFunctionFromPrompt(prompt, executionSettings: new OpenAIPromptExecutionSettings { MaxTokens = 100 });
    }
    #endregion



}