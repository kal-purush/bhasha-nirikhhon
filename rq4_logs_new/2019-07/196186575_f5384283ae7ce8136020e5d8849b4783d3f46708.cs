using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class LogIcon_AnimatorCompanion : MonoBehaviour
{
    Animator associatedAnimator;
    Image associatedImage;
    // Start is called before the first frame update
    void Start()
    {
        associatedAnimator = GetComponent<Animator>();
        associatedImage = GetComponent<Image>();
    }

    // Update is called once per frame
    void Update()
    {
        if(LoginWrapper.wrapper.listening)
        {
            associatedAnimator.SetBool("Listening",true);
            associatedImage.color = Color.green;
        }
        else
        {
            associatedAnimator.SetBool("Listening",false);
            associatedImage.color = Color.white;
        }
    }
}