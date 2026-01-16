using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

public class ArchitectFTUE : MonoBehaviour
{
    private TextMeshPro text;
    // Start is called before the first frame update
    void Start()
    {
        text = GetComponent<TextMeshPro>();
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.E) || Input.GetButtonDown("Xbox_X_Button"))
        {
            text.gameObject.SetActive(false);
        }
    }
}