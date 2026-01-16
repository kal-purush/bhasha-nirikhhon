using System.Text;
using System.Text.Json;
using DesktopAssistant.Core.Contracts.Services;
using DesktopAssistant.Core.Models;
using LiteDB;

namespace DesktopAssistant.Core.Services;

/// <summary>
/// LiteDB
/// </summary>
public class LiteDbService : ILiteDbService
{
    public IEnumerable<TodoTask> Test(string localPath) {
        // コンストラクタ引数はファイル名
        using var context = new LiteDatabase(Path.Combine(localPath, "data.db"));

        // エンティティ
        var todoTask = new TodoTask()
        {
            Title = "たいとるうううう",
            Id = 1
        };

        // DBへ接続
        var todoTasks = context.GetCollection<TodoTask>("TodoTasks");

        // ユニークインデックスの設定
        todoTasks.EnsureIndex(x => x.Id, true);

        // 作成
        todoTasks.Insert(todoTask);

        todoTask.Title = "更新したよおおおお";

        // 更新
        todoTasks.Update(todoTask);

        // 検索
        // Titleが「更」で始まるもの
        var results = todoTasks.Find(x => x.Title.StartsWith("更"));
        return results;
    }
}