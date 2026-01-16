using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesktopAssistant.Core.Models;

/// <summary>
/// Todoタスクの分類
/// </summary>
public record TaskCategory
{
    /// <summary>
    /// ID
    /// </summary>
    public long Id
    {
        get; init;
    }

    /// <summary>
    /// 分類名
    /// </summary>
    public string Name
    {
        get; set;
    }

    /// <summary>
    /// 属するタスク
    /// </summary>
    public List<TodoTask> TodoTasks
    {
        get; set;
    }
}