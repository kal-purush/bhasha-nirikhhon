using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioMaster : MonoBehaviour
{
    [SerializeField]
    AudioSource[] sources;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void PlayChainSE(int chainNum)
    {
        int target = Mathf.Min(sources.Length - 1, chainNum - 1);
        sources[target].Play();
    }
}