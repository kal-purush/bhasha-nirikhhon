using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class MovieSkip_M : MonoBehaviour
{
    public GameObject skip;
    public bool twin;
    public float doubleClick;

    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            twin = true;
        }

        if (twin)
        {
            doubleClick += Time.deltaTime;
            if (doubleClick < 0.2f)
            {
                if (Input.GetMouseButtonDown(0))
                {
                    twin = false;
                    skip.SetActive(true);
                    doubleClick = 0;
                }
            }
            else
            {
                doubleClick = 0;
                twin = false;
            }
        }


    }
}