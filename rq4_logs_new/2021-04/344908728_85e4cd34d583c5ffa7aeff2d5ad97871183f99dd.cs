using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CompleteScript : MonoBehaviour
{
    public bool complete = false;
    public bool visible = true;

    private SpriteAnimator animator;

    // Start is called before the first frame update
    void Start()
    {
        animator = GetComponent<SpriteAnimator>();
        GetComponent<Image>().enabled = visible;
    }

    // Update is called once per frame
    void Update()
    {
        if (complete)
        {
            animator.enabled = true;
            GetComponent<Image>().enabled = true;
        }
    }
}