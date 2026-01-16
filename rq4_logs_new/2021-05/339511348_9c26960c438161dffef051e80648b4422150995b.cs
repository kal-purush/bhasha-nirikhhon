using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Videoactive : MonoBehaviour
{
    public GameObject videoplayer;
    public int tts;
    // Start is called before the first frame update
    void Start()
    {
        videoplayer.SetActive(false);
    }

    void OnTriggerEnter2D (Collider2D other)
    {
        if (other.CompareTag("Player"))
        {
            Debug.Log(":v");
            videoplayer.SetActive(true);
            Destroy(videoplayer, tts);
            //StartCoroutine(Desactivatepls());
            
        }
    }
   /* IEnumerator Desactivatepls()
    {
        
        yield return new WaitForSeconds(6f);
        videoplayer.SetActive(false);
    }*/
}