using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MusicController : MonoBehaviour
{
     private static MusicController instance = null;
    // Start is called before the first frame update
    void Awake()
    {
        if(instance!=null && instance != this)
            Destroy(gameObject);
        
        instance = this;
        DontDestroyOnLoad(this.gameObject);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}