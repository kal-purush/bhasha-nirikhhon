using System.Text.Json;

namespace VRCXDiscordTracker.Core.Config;
/// <summary>
/// アプリケーションの設定を管理するクラス
/// </summary>
internal class AppConfig
{
    /// <summary>
    /// 設定ファイルのパス
    /// </summary>
    /// <remarks>アプリケーションの実行ディレクトリに config.json という名前で保存される</remarks>
    /// <example>config.json</example>
    private static readonly string _configFilePath = Path.Combine(Environment.CurrentDirectory, "config.json");

    /// <summary>
    /// 設定データ
    /// </summary>
    private static ConfigData _config = new();

    /// <summary>
    /// JSONのシリアルオプション
    /// </summary>
    private static readonly JsonSerializerOptions _jsonSerializerOptions = new()
    {
        WriteIndented = true
    };

    /// <summary>
    /// 静的コンストラクタ。設定ファイルを読み込む
    /// </summary>
    static AppConfig() => Load();

    /// <summary>
    /// 設定ファイルを読み込む
    /// </summary>
    private static void Load()
    {
        if (!File.Exists(_configFilePath))
        {
            return;
        }

        var json = File.ReadAllText(_configFilePath);
        ConfigData config = JsonSerializer.Deserialize<ConfigData>(json) ?? throw new InvalidOperationException("Failed to deserialize config file.");
        _config = config;
    }

    /// <summary>
    /// 設定ファイルを保存する
    /// </summary>
    private static void Save()
    {
        var json = JsonSerializer.Serialize(_config, _jsonSerializerOptions);
        File.WriteAllText(_configFilePath, json);
    }

    /// <summary>
    /// VRCXのデータベースファイルのパスを取得または設定するプロパティ
    /// </summary>
    /// <remarks>空白を設定すると、デフォルトのデータベースパス（%APPDATA%\VRCX\VRCX.sqlite3）が使用される</remarks>
    public static string DatabasePath
    {
        get
        {
            Load();
            return _config.DatabasePath;
        }
        set
        {
            var trimmedValue = value.Trim();
            if (string.IsNullOrEmpty(trimmedValue))
            {
                // 空白の場合はデフォルトのパスを使用
                _config.DatabasePath = AppConstants.VRCXDefaultDatabasePath;
            }
            if (!File.Exists(trimmedValue))
            {
                // File.Existsは、ファイルが存在しない場合や、アクセス権がない場合にfalseを返す
                throw new FileNotFoundException($"The specified file does not exist or not readable: {trimmedValue}");
            }
            _config.DatabasePath = trimmedValue;
            Save();
        }
    }

    /// <summary>
    /// DiscordのWebhook URLを取得または設定するプロパティ
    /// </summary>
    /// <remarks>空白を設定すると、Discordへの通知は行われない</remarks>
    public static string DiscordWebhookUrl
    {
        get
        {
            Load();
            return _config.DiscordWebhookUrl;
        }
        set
        {
            var trimmedValue = value.Trim();
            if (!string.IsNullOrEmpty(trimmedValue) && !trimmedValue.StartsWith("http://") && !trimmedValue.StartsWith("https://"))
            {
                throw new ArgumentException("DiscordWebhookUrl must start with http or https.");
            }
            _config.DiscordWebhookUrl = trimmedValue;
            Save();
        }
    }
}