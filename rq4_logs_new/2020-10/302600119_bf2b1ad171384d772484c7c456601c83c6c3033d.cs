using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class interactWithPlayer : MonoBehaviour
{

    public traceSpawner traceSpawnerPlayer;
    // Start is called before the first frame update
    void Start()
    {
        traceSpawnerPlayer = FindObjectOfType<traceSpawner>();
    }

    // Update is called once per frame
    void Update()
    {
       
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if(collision.gameObject.tag == "Player")
        {
        
            collision.gameObject.GetComponent<CharacterController2D>().speedLevel_One_bool = true;
            traceSpawnerPlayer.changeStateSpawnTraces();


        }
    }
    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            Invoke("changeSpawnTrace", 0.3f);
            
        }
    }
    void changeSpawnTrace()
    {
        traceSpawnerPlayer.changeStateSpawnTraces();
    }
}