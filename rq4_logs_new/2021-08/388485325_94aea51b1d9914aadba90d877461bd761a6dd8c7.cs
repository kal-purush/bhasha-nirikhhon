using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class EndingSceneManager : MonoBehaviour
{


    public float timer = 0f;
    public GameObject cut1;
    public GameObject cut2;
    public GameObject cut3;
    public GameObject cut4;
    public GameObject cut5;
    public Button replayB;
    public int counter = 1;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        timer += Time.deltaTime;
        if (timer >= 2.5f)
        {
            switch (counter)
            {
                case 1:
                    Destroy(cut1.gameObject);
                    break;
                case 2:
                    Destroy(cut2.gameObject);
                    break;
                case 3:
                    Destroy(cut3.gameObject);
                    break;
                case 4:
                    Destroy(cut4.gameObject);
                    break;
                case 5:
                    Destroy(cut5.gameObject);
                    replayB.interactable = true;
                    break;
            }

            timer = 0;
            counter++;
        }
    }
}