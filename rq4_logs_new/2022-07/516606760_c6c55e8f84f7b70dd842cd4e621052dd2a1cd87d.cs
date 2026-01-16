using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NoteController : MonoBehaviour
{

    public float noteSpeed;
    public bool startScrolling;
    public AudioSource music;
    public Transform beatObject;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if(!startScrolling)
        {

            transform.position -= new Vector3(0f, 0f, 0f);

            if (music.isPlaying == true)
            {
                startScrolling = true;
            }
        }else {
            transform.position -= new Vector3(noteSpeed * Time.deltaTime, 0f, 0f);
        }

        if(music.isPlaying == false)
        {
            startScrolling = false;
        }
    }
}