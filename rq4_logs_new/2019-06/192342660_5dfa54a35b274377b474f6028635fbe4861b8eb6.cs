using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class NewBehaviourScript : MonoBehaviour
{
    public string txtf;
    public Text txt;
    public Image panel;

    private int count = 0;
    private bool isDialogue = false;

    List<Dictionary<string, object>> dialogue;

    void Start()
    {
        dialogue = CSVReader.Read(txtf);
    }


    public void OnOff(bool _flag)
    {
        panel.gameObject.SetActive(_flag);
        txt.gameObject.SetActive(_flag);
    }

    public void ShowDialogue()
    {
        OnOff(true);
        count = 0;
        isDialogue = true;
        NextDialogue();
    }


    public void HideDialogue()
    {
        OnOff(false);
        isDialogue = false;
    }


    public void quitDialogue()
    {
        OnOff(false);
        isDialogue = false;
    }

    public void NextDialogue()
    {
        txt.text = (string)dialogue[count]["dialog"];
        if( (int) dialogue[count]["name"] == 1)
        {
            txt.color = Color.white;
            txt.fontSize = 15;
            GameObject.Find("메리");
            Vector3 v1 = new Vector3(0, 0, 0);

            //버튼
            var direction1 = GameObject.Find("벤").GetComponent<RectTransform>().position;
           
            //메리
            var direction2 = GameObject.Find("메리").GetComponent<RectTransform>().position;


            txt.GetComponent<RectTransform>().position = new Vector3(direction1.x - direction2.x, direction2.y+4,0);
        }
        if ((int)dialogue[count]["name"] == 2)
        {
            txt.color = Color.blue;
            txt.fontSize = 15;
            GameObject.Find("메리");
            Vector3 v1 = new Vector3(0, 0, 0);

            //버튼
            var direction1 = GameObject.Find("벤").GetComponent<RectTransform>().position;

            //메리
            var direction2 = GameObject.Find("메리").GetComponent<RectTransform>().position;


            txt.GetComponent<RectTransform>().position = new Vector3(direction1.x - direction2.x, direction2.y, 0);
        }
        if ((int)dialogue[count]["name"] == 2)
        {
            txt.color = Color.green;
            txt.fontSize = 15;
            GameObject.Find("메리");
            Vector3 v1 = new Vector3(0, 0, 0);

            //버튼
            var direction1 = GameObject.Find("벤").GetComponent<RectTransform>().position;

            //메리
            var direction2 = GameObject.Find("메리").GetComponent<RectTransform>().position;


            txt.GetComponent<RectTransform>().position = new Vector3(direction1.x - direction2.x, direction2.y, 0);
        }

        count++;
    }


    public void UpdateDial()
    {
        Update();
    }

    public void Update()
    {
        if (isDialogue)
        {
            if (Input.GetMouseButtonDown(0))
            {

                if (count < dialogue.Count)
                {
                    NextDialogue();
                }
                else
                    quitDialogue();
            }
        }

    }
}