using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Btn_Control : MonoBehaviour {
    public int page = 0;
    public string NextSceneString;
    public Text text1;
    public Text text2;
    public Text text3;

    public RawImage prev;
    public RawImage next;

    public RawImage dialog;
    public Text name;

    public RawImage park;

    public Text infoText;

    public RawImage map;
    public Text bubbleText1;
    public Text bubbleText2;

	// Use this for initialization
	void Start () {
        map.gameObject.SetActive(false);
        bubbleText2.gameObject.SetActive(false);
	}
	
	// Update is called once per frame
	void Update () {
		
        switch (page)
        {
            case 0:
                text1.gameObject.SetActive(true);
                text2.gameObject.SetActive(false);
                text3.gameObject.SetActive(false);
                prev.gameObject.SetActive(false);
                next.gameObject.SetActive(true);
                break;
            case 1:
                text1.gameObject.SetActive(false);
                text2.gameObject.SetActive(true);
                text3.gameObject.SetActive(false);
                prev.gameObject.SetActive(true);
                next.gameObject.SetActive(true);
                break;
            case 2:
                text1.gameObject.SetActive(false);
                text2.gameObject.SetActive(false);
                text3.gameObject.SetActive(true);
                prev.gameObject.SetActive(true);
                next.gameObject.SetActive(true);
                break;
            case 3:
                text1.gameObject.SetActive(false);
                text2.gameObject.SetActive(false);
                text3.gameObject.SetActive(false);
                prev.gameObject.SetActive(false);
                next.gameObject.SetActive(false);
                name.gameObject.SetActive(false);
                dialog.gameObject.SetActive(false);
                park.gameObject.SetActive(false);
                break;
            case 4:
                map.gameObject.SetActive(true);
                bubbleText1.gameObject.SetActive(false);
                bubbleText2.gameObject.SetActive(true);
                infoText.gameObject.SetActive(false);
                break;
            case 5:
                Application.LoadLevel(NextSceneString);
                break;

        }
	}
}