using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class detection : MonoBehaviour
{
  [SerializeField] public GameObject msgPanel;
  [SerializeField] Text mtext;
        // Start is called before the first frame update
    void Awake()
    {
        GetComponent<Collider>();
    }
    void Start(){
        msgPanel.SetActive(false);
    }
    // Update is called once per frame
    void OnTriggerEnter(Collider other)
    {
        if(other.name == "npc"){
        string msg = other.GetComponent<MsgNpc>().getMsg();
        mtext.text = msg;
         msgPanel.SetActive(true);
        }
    }

        void OnTriggerExit(Collider other)
    {
        if(other.name == "npc"){
         msgPanel.SetActive(false);
        }
    }
}