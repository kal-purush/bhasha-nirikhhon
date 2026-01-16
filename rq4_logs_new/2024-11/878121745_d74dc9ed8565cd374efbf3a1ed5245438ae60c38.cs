using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

public class TutnDone : MonoBehaviour
{
    public GameObject TutText;
    // Start is called before the first frame update
    void Start()
    {
        TutText.SetActive(true);
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyUp(KeyCode.Space))
        {
            TutText.SetActive(false);
        }
    }
}