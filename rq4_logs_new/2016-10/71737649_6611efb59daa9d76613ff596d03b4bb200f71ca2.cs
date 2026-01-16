using UnityEngine;
using System.Collections;
public class GetMovieFileFromLocal : MonoBehaviour {
    private MovieTexture movieTexture; //映像
    private AudioSource movieAudio; //音声
    public GameObject GameobjectForVideoTexture;
    bool movieflag = false;
    bool musicflag = false;
    public void StreamPlayVideoAsTexture(string OggVideoURL,string MusicURL)
    {
        movieflag = System.IO.File.Exists(System.IO.Directory.GetCurrentDirectory() + OggVideoURL);
        musicflag = System.IO.File.Exists(System.IO.Directory.GetCurrentDirectory() + MusicURL);
        StartCoroutine(StartStream("file://"+System.IO.Directory.GetCurrentDirectory()+OggVideoURL,"file://"+System.IO.Directory.GetCurrentDirectory()+MusicURL));
    }

    protected IEnumerator StartStream(string url,string surl)
    {
        //エラー2つ出るが無視してOK
        //https://issuetracker.unity3d.com/issues/movietexture-fmod-error-when-trying-to-play-video-using-www-class
        //Debug.Log("Ignore following two errors");
        WWW videoStreamer = new WWW(url);
        WWW musicStreamer = new WWW(surl);

        if (movieflag)
        {
            movieTexture = videoStreamer.movie;
            while (!movieTexture.isReadyToPlay)
            {
                yield return 0;
            }
            GameobjectForVideoTexture.GetComponent<Renderer>().material.mainTexture = movieTexture;
            movieflag = true;
        }
        if (musicflag)
        {
            movieAudio = GameobjectForVideoTexture.AddComponent<AudioSource>();
            movieAudio.clip = movieTexture.audioClip;
            movieAudio.playOnAwake = false;
            musicflag = true;
        }
        //movieTexture.Play();
        //audioSource.Play();
    }
    // Update is called once per frame
    void Update()
    {
        //Jumpボタン(スペースキー)を押したら
        if (Input.GetButtonDown("Jump"))    //デフォルトではJumpはスペースキーに割り当て(Edit>>Project Stting>>Input)
        {
            if (movieflag)
            {
                if (movieTexture.isPlaying) //動画の再生中
                {
                    //動画と音声を止める
                    movieTexture.Pause();

                }
                else
                {
                    //動画と音声を再生する
                    movieTexture.Play();
                }
            }
            if (musicflag)
            {
                if (movieAudio.isPlaying)
                {
                    movieAudio.Pause();
                }else
                {
                    movieAudio.Play();
                }
            }
        }
    }
}