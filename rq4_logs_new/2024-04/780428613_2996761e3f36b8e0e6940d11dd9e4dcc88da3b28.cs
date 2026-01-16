using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesktopAssistant.Core.Models;

/// <summary>
/// キャラクター
/// </summary>
public record Character
{
    /// <summary>
    /// ID
    /// </summary>
    public long Id
    {
        get; init;
    }

    /// <summary>
    /// 名前
    /// </summary>
    public string Name
    {
        get; init;
    }

    /// <summary>
    /// プロンプト
    /// </summary>
    public string Prompt
    {
        get; init;
    }

    /// <summary>
    /// 会話のリスト
    /// </summary>
    public ICollection<Chat> Chats
    {
        get; set;
    }
}