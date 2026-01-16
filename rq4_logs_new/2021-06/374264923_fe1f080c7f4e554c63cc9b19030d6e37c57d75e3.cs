using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RainBlockInitialize : MonoBehaviour
{
    public GameObject [] umbrellaPaths;
    int umbrellaPath;
    // Start is called before the first frame update
    void Start()
    {
        System.Random random = new System.Random();
        umbrellaPath = random.Next(0,umbrellaPaths.Length);
        Instantiate(umbrellaPaths[umbrellaPath], this.transform, true);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}