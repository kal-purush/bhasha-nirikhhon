namespace MinimalChessApi.Services
{
    public class FileStoreService : IGameStoreService
    {
        private string _targetPath;
        private const string GameExtension = "game";

        public FileStoreService(string targetPath)
        {
            _targetPath = targetPath;

            // it's only for testing, so don't spam everything with files.
            // create a clean dir every time
            SetupCleanTargetDir();
        }
        private Task SetupCleanTargetDir()
        {
            if (Directory.Exists(_targetPath))
            {
                Directory.Delete(_targetPath, true);
            }

            Directory.CreateDirectory(_targetPath);

            return Task.CompletedTask;
        }

        private string GetGameFilename(Guid gameId)
        {
            return Path.Combine(_targetPath, $"{gameId}.{GameExtension}");
        }

        public Task<string?> LoadGameAsync(Guid gameId)
        {
            var filename = GetGameFilename(gameId);
            if (File.Exists(filename))
            {
                try
                {
                    return File.ReadAllTextAsync(filename)!;
                }
                catch { }
            }

            return Task.FromResult(null as string);
        }

        public async Task<bool> SaveGameAsync(Guid gameId, string game)
        {
            try
            {
                var filename = GetGameFilename(gameId);
                await File.WriteAllTextAsync(filename, game);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public Task<List<Guid>?> GetGamesAsync()
        {
            if (Directory.Exists(_targetPath))
            {
                var result = new List<Guid>();
                foreach (var file in Directory.GetFiles(_targetPath, $"*.{GameExtension}"))
                {
                    var guidCandidate = Path.GetFileNameWithoutExtension(file);
                    if (Guid.TryParse(guidCandidate, out var guid))
                    {
                        result.Add(guid);
                    }
                }

                return Task.FromResult(result)!;
            }

            return Task.FromResult(null as List<Guid>);
        }
    }
}