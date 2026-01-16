using SonicBloom.Koreo;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class KoreoEventSubcriber : MonoBehaviour
{
    public delegate void KoreographyEventCallback(KoreographyEvent koreoEvent);
    // Start is called before the first frame update
    void Start()
    {
        Koreographer.Instance.RegisterForEvents("Base_BPM-Drums", FireEventWDebg);
    }
    void FireEventWDebg(KoreographyEvent koreoE)
    {
        Debug.Log("Koreo Event fired");
    }
    // Update is called once per frame
    void Update()
    {
        
    }
}