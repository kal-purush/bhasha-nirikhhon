using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Block : MonoBehaviour
{
    //parameter hay shekl bandi
    public AudioClip breakSound; // seday nabood shodan
    public GameObject blockvfx; // jelve hay visze baad nabood shodan blocks
    public int maxHit; // hitpoint
    public Sprite[] Damageshower;
    // cached references
    level level;
    GameSession currentpoints;
    //state variables
    public int timeHit; //har zaman ke barkhord mikonad
    private void Start()
    {
        countBreakableBlocks();
    }

    private void countBreakableBlocks()
    {
        level = FindObjectOfType<level>();
        currentpoints = FindObjectOfType<GameSession>();
        // it means just count the breakable blocks
        if (tag == "Breakable")
        {
            
            level.countBlocks();
        }
      
    }

    // dar zaman az bein raftan
    private void OnCollisionEnter2D(Collision2D collision)
    {
        //it means just destroy the breakable blocks
        if (tag == "Breakable")
        {
            HittingTime();
        }
        else if (tag == "Damagedblock")
        {
            HittingTime();
        }
    }

    private void HittingTime()
    {
        timeHit++;
        if (timeHit >= maxHit)
        {
            destroyblock();
        }
        else showthedamagedblock();
        }

    private void showthedamagedblock()
    {   
        
            int Spriteindex = timeHit - 1;
            GetComponent<SpriteRenderer>().sprite = Damageshower[Spriteindex];
        
     }

    private void destroyblock()
    {
        Destroyedblockssfx();
        Destroy(gameObject);
        level.destroyedobject();
        blockvfxtrigger();

    }
    // marboot be audio
    private void Destroyedblockssfx()
    {
        AudioSource.PlayClipAtPoint(breakSound, Camera.main.transform.position);
        currentpoints.Addingupponts();
    }
    // marboot be jelve hay visze
    private void blockvfxtrigger()
    {
        GameObject sparkles = Instantiate(blockvfx, transform.position, transform.rotation);
        Destroy(sparkles, 1f);
    }
}