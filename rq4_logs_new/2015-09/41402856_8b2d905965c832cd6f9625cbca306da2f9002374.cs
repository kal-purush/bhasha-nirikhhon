using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;
using System.Collections;

public class UIController : MonoBehaviour, IPointerEnterHandler, IPointerDownHandler
{
    public AudioClip hover;
    public AudioClip click;
    private AudioSource audio;
    private AudioSource music;

    public void Start()
    {
        audio = GetComponent<AudioSource>();
    }

    public void OnPointerEnter(PointerEventData ped)
    {

        audio.PlayOneShot(hover);
    }

    public void OnPointerDown(PointerEventData ped)
    {
        audio.PlayOneShot(click);
        if (gameObject.name == "Start UI")
        {
            music = GameObject.Find("Music Player").GetComponent<AudioSource>();
            music.Stop();
        }
    }
}