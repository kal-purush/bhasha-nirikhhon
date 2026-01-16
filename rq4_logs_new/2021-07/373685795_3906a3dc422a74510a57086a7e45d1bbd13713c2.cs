using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class DPadPad : MonoBehaviour
{
    public EmoteSending emoteSender;

    public Image uiEmoteReceiveRecent;

    public Image[] emoteButtons;

    // Start is called before the first frame update
    void Awake()
    {
        emoteSender.dPad = this;

        for (int i = 0; i < 4; i++)
        {
            emoteButtons[i].sprite = emoteSender.emoteSprites[i];
        }
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void DPadSend(int emote)
    {
        emoteSender.ToDiver(emote);
    }

    public void DPadReceive(int emote)
    {

    }
}