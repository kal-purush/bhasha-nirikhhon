using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class hitbox : MonoBehaviour
{
    private float rangePerfect, rangeGood;
    public string playerName = "Player";
    private GameObject player;
    private int state = 0; // 0 for inactive, 1 for active but not pressed, 2 for inactive and pressed

    void Start()
    {
        player = GameObject.Find(playerName);
        rangeGood = 1.3f;
        rangePerfect = 0.7f;
    }

    // Update is called once per frame
    void Update()
    {
        float dist = Mathf.Abs(transform.position.x - player.transform.position.x);

        if (state == 0)
        {
            if (dist <= rangeGood) { state = 1; }
        } else if (state == 1)
        {
            if (dist < rangePerfect && Input.GetKeyDown(KeyCode.F)) { state = 2; Debug.Log("Perfect"); }
            else if (dist < rangeGood && Input.GetKeyDown(KeyCode.F)) { state = 2; Debug.Log("Good"); }
            else if (dist > rangeGood) { state = 2; Debug.Log("Miss"); }
        }
    }
}