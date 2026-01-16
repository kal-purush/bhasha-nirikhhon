using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ButtonTest : MonoBehaviour
{
    private GameObject npc;

    // Start is called before the first frame update
    void Start()
    {
        npc = transform.root.gameObject;
        gameObject.SetActive(false);
    }


    // Update is called once per frame
    void Update()
    {
    }

    private void OnMouseDown()
    {
        if (npc.tag == "Portal")
        {
            npc.GetComponent<changeScene>().changeFirstScene();
        }
        else
        {
            npc.GetComponent<DialogueSystemTest>().ShowDialogue();
        }

        //gameObject.SetActive(false);
    }

}