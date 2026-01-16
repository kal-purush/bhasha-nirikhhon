using System.Threading.Tasks;
using Newtonsoft.Json;
using KaraIOke.Models;
using Windows.ApplicationModel.VoiceCommands;

namespace KaraIOke.Services.Download;

public class SongAudio
{
    public byte[] Vocals { get; set; } = [];
    public byte[] NoVocals { get; set; } = [];
}

public class DownloadService : IDownloadService
{
    private Dictionary<string, Task> _songDownloads = new();
    private Dictionary<string, SongAudio> _songAudios = new();

    private HttpClient _client = new HttpClient();
    public DownloadService()
    {
        _client.BaseAddress = new Uri("http://localhost:8000/");
    }

    public void queryDownload(Song song)
    {
        _client.PostAsync($"v1/process_song?link={song.url}", null)
            .ContinueWith(async responseTask =>
            {
                var response = await responseTask;
                var responseBody = await response.Content.ReadAsStringAsync();
                var songID = JsonConvert.DeserializeObject<SongID>(responseBody) ?? new();
                song.hash = songID.song_id;

                if (_songDownloads.ContainsKey(song.hash))
                    return;

                _songDownloads.Add(song.hash, waitForSong(song));
            });
    }

    private async Task<bool> pollSong(Song song)
    {
        var response = await _client.GetAsync($"v1/songinfo/{song.hash}");
        var responseBody = await response.Content.ReadAsStringAsync();
        var metaData = JsonConvert.DeserializeObject<MetaData>(responseBody) ?? new();
        return metaData.ready;
    }

    private async Task<byte[]> getAudio(Song song, string path)
    {
        var response = await _client.GetAsync($"v1/{path}/{song.hash}");
        return await response.Content.ReadAsByteArrayAsync();
    }

    private async Task<string> waitForSong(Song song)
    {
        while (!await pollSong(song))
        {
            Thread.Sleep(1000);
        }

        var vocals = getAudio(song, "song_vocals");
        var noVocals = getAudio(song, "song_no_vocals");

        _songAudios.Add(song.hash, new SongAudio { Vocals = await vocals, NoVocals = await noVocals });

        return song.hash;
    }
}