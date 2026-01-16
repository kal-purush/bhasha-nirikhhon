using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;
using static UnityEditor.Progress;

public class DropGold : MonoBehaviour
{
    private float timer;
    public float cooldown;
    public GameObject item;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (timer < Time.time)
        {
            timer = cooldown + Time.time;
            GameObject drop = Instantiate(item);
            drop.transform.position = transform.position;
        }
    }
}