using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ControlAppear : MonoBehaviour
{
    public GameObject[] appears;
    public GameObject[] delete;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void AppearDelete()
    {
        for (int i = 0; i < appears.Length; i++)
        {
            appears[i].SetActive(true);
        }
        for (int j = 0; j < delete.Length; j++)
        {
            delete[j].SetActive(false);
        }
        this.gameObject.SetActive(false);
    }
}