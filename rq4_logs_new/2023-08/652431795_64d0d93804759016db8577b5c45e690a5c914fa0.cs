using System.Text.Json;
using Microsoft.Extensions.FileProviders;
using Nameless.Localization.Microsoft.Json.Objects;

namespace Nameless.Localization.Microsoft.Json.Infrastructure.Impl {
    public sealed class FileTranslationManager : ITranslationManager {
        #region Private Records

        private record CacheEntry(string Culture, Translation Content, IDisposable? FileChangeHandler = null);

        #endregion

        #region Private Static Read-Only Fields

        private static readonly Dictionary<string, CacheEntry> Cache = new();

        #endregion

        #region Private Read-Only Fields

        private readonly IFileProvider _fileProvider;
        private readonly LocalizationOptions _options;

        #endregion

        #region Public Constructors

        public FileTranslationManager(IFileProvider fileProvider, LocalizationOptions? options = null) {
            _fileProvider = Prevent.Against.Null(fileProvider, nameof(fileProvider));
            _options = options ?? LocalizationOptions.Default;
        }

        #endregion

        #region Private Methods

        private void HandleFileChange(object? state) {
            if (state == null) { return; }

            var culture = (string)state;

            if (Cache.Remove(culture, out var entry)) {
                entry.FileChangeHandler?.Dispose();
            }
        }

        private bool TranslationFileExists(string culture) {
            var path = GetTranslationFilePath(culture);
            var file = _fileProvider.GetFileInfo(path);
            return file.Exists;
        }

        private string GetTranslationFilePath(string culture) {
            var path = Path.Combine(_options.TranslationFolder, $"{culture.ToLowerInvariant()}.json");
            return path;
        }

        private string GetFileContent(string path) {
            var file = _fileProvider.GetFileInfo(path);
            using var reader = new StreamReader(file.CreateReadStream());
            var content = reader.ReadToEnd();
            return content; ;
        }

        private IDisposable? CreateFileChangeHandler(string culture, string path) {
            if (!_options.WatchFileForChanges) {
                return null;
            }

            var changeToken = _fileProvider.Watch(path);
            var handler = changeToken.RegisterChangeCallback(HandleFileChange, culture);

            return handler;
        }

        private CacheEntry CreateCacheEntry(string culture) {
            var path = GetTranslationFilePath(culture);
            var fileContent = GetFileContent(path);
            var fileChangeHandler = CreateFileChangeHandler(culture, path);

            var translation = JsonSerializer.Deserialize<Translation>(fileContent, new JsonSerializerOptions {
                Converters = { TranslationJsonConverter.Default }
            }) ?? throw new InvalidOperationException($"Couldn't deserialize translation file. Culture: {culture}");

            return new(culture, translation, fileChangeHandler);
        }

        #endregion

        #region ITranslationManager Members

        public Translation GetTranslation(string culture) {
            Prevent.Against.NullOrWhiteSpace(culture, nameof(culture));

            if (!TranslationFileExists(culture)) {
                return Translation.Empty;
            }

            if (!Cache.ContainsKey(culture)) {
                var entry = CreateCacheEntry(culture);
                Cache.Add(culture, entry);
            }

            return Cache[culture].Content;
        }

        #endregion
    }
}