using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class RaycastSelection : MonoBehaviour
{

    public Camera mainCam; // it's not neccessary because we can get our camera from Camera.main
    public string tagName;
    private QuestItem qiHit;

    //public Text objectText; // it displays object name

    //private CanvasGroup canvasGroup; // we use it to hide canvas 
    //private GameObject lastSelected = null; // store last selected object reference

    void Start()
    {
        //canvasGroup = GetComponent<CanvasGroup>();
        //canvasGroup.alpha = 0f; // hide canvas
        Debug.Log("raycast running start");
    }

    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        { // if LMB clicked
            Debug.Log("raycast detect left click");
            bool selectionHit = false;
            RaycastHit raycastHit = new RaycastHit(); // create new raycast hit info object
            if (Physics.Raycast(mainCam.ScreenPointToRay(Input.mousePosition), out raycastHit))
            { // create ray from screen's mouse position to world and store info in raycastHit object
                Debug.Log("raycast found camera");
                if (raycastHit.collider.tag == tagName)
                { // we just want to hit objects tagged with the tagname
                    Debug.Log("Found raycast!" +tagName);
                    selectionHit = true; // yup, we hit it!
                }
            }

            if (selectionHit) {
                //Debug.Log("raycastHit.collider.gameObject's name ")
                qiHit = raycastHit.collider.gameObject.GetComponent<QuestItem>();
                qiHit.fire();
            }
        }
    }

   
}