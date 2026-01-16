using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
namespace ScoreBoard
{
    public class ReadJson : IReadData
    {
        public async Task<List<T>> ReadAsync<T>(string path)
        {
            JsonSerializerOptions _options = new()
            {
                PropertyNameCaseInsensitive = true
            };

            var json = await File.ReadAllTextAsync(path);
            return JsonSerializer.Deserialize<List<T>>(json, _options);
        }
    }
}