using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class dialogue : MonoBehaviour           
{
    // Start is called before the first frame update
    [SerializeField]
    GameObject Dial;
    [SerializeField]
    Text titleUi, dialUi;
     [SerializeField]
    string title;
    [SerializeField]
    [TextArea(3,10)]
    string [] myDial;
    bool dialogueIsStart=false;
    int countSentence=0;
    
    

    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        startDialogue();
    }
    private void OnTriggerEnter2D(Collider2D other) {
        if(other.tag=="Player"){
            
            
            dialogueIsStart=true;
            countSentence=0;
            dialUi.text=myDial[countSentence];
            countSentence++;


        }
    }
    private void OnTriggerExit2D(Collider2D other) {
        if(other.tag=="Player"){
           countSentence=0;
           dialUi.text=myDial[countSentence];
            Dial.SetActive(false);
            dialogueIsStart=false;
        }
        
    }

    void startDialogue(){
            titleUi.text=title;
            
        if(dialogueIsStart){
            Dial.SetActive(true);
            titleUi.text=title;
            if(Input.GetKeyDown(KeyCode.E)){
                if(countSentence<myDial.Length){ 
                dialUi.text=myDial[countSentence];
                    countSentence++;
                }
                
            }

        }
    }
    
}