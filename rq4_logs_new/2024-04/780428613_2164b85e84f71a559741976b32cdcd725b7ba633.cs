using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesktopAssistant.Core.Models;

/// <summary>
/// 完全固定のシステムイベント
/// 設定ファイルでやっても良いが、今回はDBで管理する
/// </summary>
public enum SystemEvents
{
    /// <summary>
    /// 初回起動：タスク登録
    /// </summary>
    Initial_SetTasks = 0,

    /// <summary>
    /// 初回起動：キャラクター登録
    /// </summary>
    Initial_SetCharacoers = 1,
}

/// <summary>
/// このシステムのイベント
/// </summary>
public record SystemEvent
{
    /// <summary>
    /// ID
    /// </summary>
    public long Id
    {
        get; init;
    }

    /// <summary>
    /// 題名
    /// </summary>
    public SystemEvents Event
    {
        get; set;
    }

    /// <summary>
    /// 内容
    /// </summary>
    public string Content
    {
        get; set;
    }

    /// <summary>
    /// タスクが完了しているかどうか
    /// </summary>
    public bool IsDone
    {
        get; set;
    }

    /// <summary>
    /// 作成日時
    /// </summary>
    public DateTime CreatedAt
    {
        get; init;
    }

    /// <summary>
    /// 更新日時（完了日時）
    /// </summary>
    public DateTime UpdatedAt
    {
        get; set;
    }
}