using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BirdScript : MonoBehaviour
{

    public LogicScript logic;

    public Rigidbody2D myRigidbody;
    public float flapStrength;
    public AudioSource deadSFX;

    public bool birdIsAlive = true;
    // Start is called before the first frame update
    void Start()
    {
        logic = GameObject.FindGameObjectWithTag("Logic").GetComponent<LogicScript>();
        deadSFX = GetComponent<AudioSource>();
    }

    // Update is called once per frame
    void Update()
    {

        if(Input.GetKeyDown(KeyCode.Space) == true && birdIsAlive)
        {
            myRigidbody.velocity = Vector2.up * flapStrength;
        }

        if( birdIsAlive == false){
            
            myRigidbody.velocity = Vector2.up * -50;
        }
        
    }

    private void OnCollisionEnter2D(Collision2D collision){
        logic.gameOver();
    
        birdIsAlive = false;
        deadSFX.Play();
    }
}