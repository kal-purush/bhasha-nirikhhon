using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Video;

public class PreviewAgent : MonoBehaviour
{
    [SerializeField] RawImage _leftScreen;
    [SerializeField] RawImage _left2Screen;
    [SerializeField] RawImage _rightScreen;
    [SerializeField] RawImage _right2Screen;
    [SerializeField] RawImage _screen;

    [SerializeField] VideoPlayer _mainVideoPlayer;
    [SerializeField] VideoPlayer _subVideoPlayer;

    // Start is called before the first frame update
    void Start()
    {
        StartCoroutine(PrepareVideo());
    }

    // Update is called once per frame
    void Update()
    {

    }

    IEnumerator PrepareVideo()
    {
        _subVideoPlayer.Prepare();
        while (!_subVideoPlayer.isPrepared)
        {
            yield return new WaitForSeconds(1);
            break;
        }
        _leftScreen.texture = _subVideoPlayer.texture;
        _left2Screen.texture = _subVideoPlayer.texture;
        _rightScreen.texture = _subVideoPlayer.texture;
        _right2Screen.texture = _subVideoPlayer.texture;
        _screen.texture = _subVideoPlayer.texture;
        _subVideoPlayer.Play();

    }

    public void UpdateVideo(string path)
    {
        _subVideoPlayer.Stop();
        _subVideoPlayer.url = path;
        StartCoroutine(LoadNewVideo());

    }

    IEnumerator LoadNewVideo()
    {
        _subVideoPlayer.Prepare();
        while (!_subVideoPlayer.isPrepared)
        {
            yield return new WaitForSeconds(1);
            break;
        }
        _leftScreen.texture = _subVideoPlayer.texture;
        _left2Screen.texture = _subVideoPlayer.texture;
        _rightScreen.texture = _subVideoPlayer.texture;
        _right2Screen.texture = _subVideoPlayer.texture;
        _screen.texture = _subVideoPlayer.texture;
        _subVideoPlayer.Play();

    }
}