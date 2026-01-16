using System.Collections.ObjectModel;
using KaraIOke.Models;

namespace KaraIOke.Services.Playlists;

public class PlaylistMockService : IPlaylistService
{
    private Dictionary<string, Playlist> playlists = new Dictionary<string, Playlist>();

    public PlaylistMockService()
    {
        for (int i = 0; i < 5; i++)
        {
            string playlistName = $"Mock playlist {i}";
            var songs = Enumerable.Range(1, 5)
                .Select(i => new Song { Name = $"Mock song {i}" })
                .ToList();
            playlists.Add(playlistName, new Playlist(playlistName, new ObservableCollection<Song>(songs)));
        }
    }
    public Playlist GetPlaylist(string playlistName)
    {
        return playlists[playlistName];
    }

    public ObservableCollection<string> GetAllPlaylistsNames()
    {
        var names = playlists.Keys.ToList();
        return new ObservableCollection<string>(names);
    }

    public void DeletePlaylist(string playlistName)
    {
        playlists.Remove(playlistName);
    }

    public void DeleteSong(string playlistName, Song song)
    {
        Playlist playlist = playlists[playlistName];
        ObservableCollection<Song> songs = playlist.Songs;
        songs.Remove(song);
    }
}