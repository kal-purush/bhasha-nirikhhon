using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;

[RequireComponent(typeof(AudioSource))]
public class CutsceneScript : MonoBehaviour
{
    public enum FrameEnder
    {
        mouse, time
    }

    [System.Serializable]
    public class CutsceneEvent
    {
        [Tooltip("Use this to handle events that occur during this cutscene frame.")]
        public UnityEvent events;
        [Tooltip("If you would like to play avoice reading or sound effect during this cutscene frame, enable this.")]
        public bool audioEvent;
        [Tooltip("Enter the audio that will play here.")]
        public AudioClip audioClip;
        [Tooltip("If you would like subtitles for the event, enable this.")]
        public bool useSubtitles;
        [Tooltip("This is the text that will be used for subtitles.")]
        public UnityEngine.UI.Text subtitleBox;
        [Tooltip("Enter the subtitles to be displayed for this event.")]
        public string subtitles;
        [Tooltip("Choose how to end the frame.")]
        public FrameEnder frameEnder;
        [Tooltip("If time has been choosen, choose how long it will be until will end after audio play.")]
        public float waitTime;
    }

    [Tooltip("If true, this cutscene will play on start. If this is left untrue, the variable 'playCutscene' must be set to play through an event or a script.")]
    public bool playOnStart;
    [Tooltip("This determines how the cutscene will play out by using a system similar to a play script. Each element is similar to an event in the script that will play out.")]
    public List<CutsceneEvent> cutsceneDirector = new List<CutsceneEvent>();

    [HideInInspector]
    public bool playCutscene;

    private AudioSource audSource;
    [HideInInspector]
    public int currentFrame;

    private void Start()
    {

        if (playOnStart) playCutscene = true;
        audSource = gameObject.GetComponent<AudioSource>();
    }

    private void Update()
    {
        if(playCutscene) StartCoroutine(CutsceneAction());
    }

    IEnumerator CutsceneAction()
    {
        for (int i = 0; i > cutsceneDirector.Count - 1; i--)
        {
            //sets the current frame variable so other scripts know what to take.
            currentFrame = i;
            //activate the events if there are any
            cutsceneDirector[i].events.Invoke();
            //if there are subtitles, add them
            if(cutsceneDirector[i].useSubtitles) cutsceneDirector[i].subtitleBox.text = cutsceneDirector[i].subtitles;
            //if there is an audio track, play it
            if (cutsceneDirector[i].audioEvent) yield return StartCoroutine(WaitForAudio(i));
            //remove subtitles once audio is done
            if (cutsceneDirector[i].useSubtitles) cutsceneDirector[i].subtitleBox.text = "";
            //depending on exit method, we will either wait for a few a seconds or wait for mouse down
            if (cutsceneDirector[i].frameEnder == FrameEnder.mouse) yield return StartCoroutine(WaitForMouseDown());
            else if (cutsceneDirector[i].frameEnder == FrameEnder.time) yield return StartCoroutine(WaitForTimer(i));
        }
    }

    IEnumerator WaitForAudio(int i)
    {
        audSource.clip = cutsceneDirector[i].audioClip;
        audSource.Play();
        yield return new WaitForSeconds(cutsceneDirector[i].audioClip.length);
    }

    IEnumerator WaitForMouseDown()
    {
        while(true)
        {
            if (Input.GetButton("Fire1")) yield break;
            else yield return null;
        }
    }

    IEnumerator WaitForTimer(int i)
    {
        yield return new WaitForSeconds(cutsceneDirector[i].waitTime);
    }
}