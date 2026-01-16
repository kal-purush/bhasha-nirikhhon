using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WallFadeout : MonoBehaviour
{
    // Start is called before the first frame update

    public Animator wallanim;
    private int current_num;
    void Start()
    {
        wallanim = GetComponent<Animator>();
    }

    private void call_scenes_current()
    {
        SceneManager scenes_ = GameObject.Find("Scenes").GetComponent<SceneManager>();
        current_num = scenes_.current;
    }
    private void FixedUpdate()
    {
        call_scenes_current();
        if(current_num == 0)
        {
            wallanim.Play("WallFadeout");
        }
        else
        {
            wallanim.Rebind();
        }
    }
}