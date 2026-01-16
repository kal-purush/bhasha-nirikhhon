using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class LibrarySongElementUI : MonoBehaviour
{
    public Button addSongToPlaylist;
    public Text label;

    private Playlist playlist; 

    public void Start()
    {
        addSongToPlaylist.onClick.AddListener(AddSongToPlaylist);

        playlist = GameObject.Find("PlaylistController").GetComponent<PlaylistController>().playlist; 
    }

    public void AddSongToPlaylist()
    {


        Debug.Log("Adding song to Playlist : " + label.text);
    
        if (MusicLibrary.Instance.library.ContainsKey(label.text))
        { 
         
            var song = new GameObject().AddComponent<Song>().Initialize("Song1", "artist1", null);


            playlist.playListSongs.Add(song);

        }
        
    }
}