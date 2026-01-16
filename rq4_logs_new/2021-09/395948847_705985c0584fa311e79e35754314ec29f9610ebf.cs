using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GradeObjectSound : MonoBehaviour
{
    private AudioSource audioSource;
    public AudioClip Fanfale;

    // Start is called before the first frame update
    void Start()
    {
        audioSource = GetComponent<AudioSource>();
        v.grade_se_fg = true;
        v.se_timer = 3.5f;
    }

    // Update is called once per frame
    void Update()
    {
        v.se_timer -= Time.deltaTime;
        if(v.se_timer < 0 && v.grade_se_fg)
        {
            v.grade_se_fg = false;
            audioSource.PlayOneShot(Fanfale);
        }
    }
}