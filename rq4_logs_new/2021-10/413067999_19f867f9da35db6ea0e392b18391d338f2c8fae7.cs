using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Text.Json;
using System.Text.Json.Serialization;
using System.IO;
using System.Text.Encodings.Web;
using System.Text.Unicode;

namespace TaskTimeDB
{
    static class Config
    {
        public static ConfigImpl config = new ConfigImpl();

        public static string DBFilePath
        {
            get { return config.json.DBFilePath; }
            set
            {
                config.json.DBFilePath = value;
            }
        }

        public static string LogDirPath
        {
            get { return config.json.LogDirPath; }
            set
            {
                config.json.LogDirPath = value;
            }
        }

        public static async Task Load()
        {
            await config.Load().ConfigureAwait(false);
        }
        public static async Task Save()
        {
            await config.Save().ConfigureAwait(false);
        }
    }

    class ConfigImpl : IDisposable
    {
        private string configFilePath;
        public JsonItem json;

        public ConfigImpl()
        {
            // パス設定
            configFilePath = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName + @".json";
        }

        /** 初回起動用に同期的に動作する
         * 
         */
        public async Task Load()
        {
            // 設定ロード
            if (File.Exists(configFilePath))
            {
                // ファイルが存在する
                //
                var options = new JsonSerializerOptions
                {
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All),
                    //Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                    //NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.AllowNamedFloatingPointLiterals,
                };
                //
                using (var stream = new FileStream(configFilePath, FileMode.Open, FileAccess.Read))
                {
                    // 呼び出し元でWait()している。ConfigureAwait(false)無しにawaitするとデッドロックで死ぬ。
                    json = await JsonSerializer.DeserializeAsync<JsonItem>(stream, options).ConfigureAwait(false);
                }
            }
            else
            {
                // ファイルが存在しない
                json = new JsonItem
                {
                    DBFilePath = "",
                    LogDirPath = "",
                };
            }
        }

        public async Task Save()
        {
            var options = new JsonSerializerOptions
            {
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                WriteIndented = true,
            };
            //
            string jsonStr = JsonSerializer.Serialize(json, options);
            //
            using (var stream = new FileStream(configFilePath, FileMode.Create, FileAccess.Write))
            {
                // 呼び出し元でWait()している。ConfigureAwait(false)無しにawaitするとデッドロックで死ぬ。
                await JsonSerializer.SerializeAsync(stream, json, options).ConfigureAwait(false);
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // 重複する呼び出しを検出するには

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Save().Wait();
                }

                disposedValue = true;
            }
        }

        void IDisposable.Dispose()
        {
            Dispose(true);
        }
        #endregion
    }

    class JsonItem
    {
        [JsonPropertyName("db_file_path")]
        public string DBFilePath { get; set; }

        [JsonPropertyName("log_dir_path")]
        public string LogDirPath { get; set; }
    }
}