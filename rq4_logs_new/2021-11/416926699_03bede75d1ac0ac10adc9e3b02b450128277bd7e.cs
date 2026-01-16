using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayListExample : MonoBehaviour
{

    public PlayList playList; 

    public List<Song> songs ; 
    private void Start()
    {

        songs = new List<Song>();



        var song1 = new GameObject().AddComponent<Song>().Initialize("Song1", "artist1", null);

        var song2 = new GameObject().AddComponent<Song>().Initialize("Song2", "artist1", null);
        var song3 = new GameObject().AddComponent<Song>().Initialize("Song3", "artist1", null);
        var song4 = new GameObject().AddComponent<Song>().Initialize("Song4", "artist1", null);

        songs.Add(song1);
        songs.Add(song2);
        songs.Add(song3);
        songs.Add(song4);


        var go = new GameObject();
        go.AddComponent<PlayList>();
        go.name = "PlayList";

        playList = go.GetComponent<PlayList>();

        playList.playListSongs = songs;





    }
}