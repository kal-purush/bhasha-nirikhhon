using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Background : MonoBehaviour
{
    public GameObject garbageObject;
    public Sprite[] sprites;

    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        int r = Random.Range(0, 100);
        if (r < 6)
        {
            GameObject obj = Instantiate(garbageObject, transform);
            Vector3 pos = obj.transform.position;
            pos.y += Random.Range(-5.0f, 5.0f);
            obj.transform.position = pos;
            obj.GetComponent<SpriteRenderer>().sprite = sprites[r];
        }
    }
}