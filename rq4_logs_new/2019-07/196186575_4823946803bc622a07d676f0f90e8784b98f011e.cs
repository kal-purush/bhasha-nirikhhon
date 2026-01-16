using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public enum emotion
{
    neutral = 0,
    happy = 1,
    sad = 2
}

public class DialogueManager : MonoBehaviour
{
    // Start is called before the first frame update
    public static DialogueManager instance;
    public string line;
    public float speakSpeed = 0.05f;
    public bool speaking = false;
    public bool finishedSpeaking = false;
    public emotion drEmotion = emotion.neutral;
    //public List<Animator> emotionAnimations;
    void Start()
    {
        if(instance == null)
        {
            instance = this;
        }
        else if (instance != this)
        {
            Destroy(this);
        }
        //foreach(GameObject obj in GameObject.FindGameObjectsWithTag("Expressive"))
        //{
        //    emotionAnimations.Add(obj.GetComponent<Animator>());
        //}
        
    }

    // Update is called once per frame
    void Update()
    {
        /* 
        if(LoginWrapper.wrapper.emotionScore <= 0.1f && LoginWrapper.wrapper.emotionScore >= -0.1f)
            drEmotion = emotion.neutral;
        else if (LoginWrapper.wrapper.emotionScore > 0.01f)
            drEmotion = emotion.happy;
        else
            drEmotion = emotion.sad;

        foreach (Animator animator in emotionAnimations)
        {
            animator.SetInteger("ExpressionIndex",(int)drEmotion);
        }
        */
    }
}