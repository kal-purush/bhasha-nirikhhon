using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BossAnimationEvent : MonoBehaviour
{
    [SerializeField] GameObject BossDown;
    // Start is called before the first frame update
    void Start()
    {
        BossDown.SetActive(false);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
    public void DownEffectActive()
    {
        BossDown.SetActive(true);
    }
    public void DownEffectFalse()
    {
        BossDown.SetActive(false);
    }
}