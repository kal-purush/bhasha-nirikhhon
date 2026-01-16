using System.Collections;
using System.Collections.Generic;
using UnityEngine;



public class SetTeleport : MonoBehaviour
{
    private GameObject levelMagnager; // Prefab of the LevelMagnager object
    private GameObject UIMagnager; // Prefab of the UIMagnager object

    // Start is called before the first frame update
    void Start()
    {
        levelMagnager = GameObject.FindGameObjectWithTag("LevelManager");
        UIMagnager = GameObject.FindGameObjectWithTag("UIManager");
        for(int i = 0; i < levelMagnager.GetComponent<TeleportTracker>().telea.Length; i++){

                //Debug.Log(levelMagnager.GetComponent<TeleportTracker>().telea[i]);


                if (UIMagnager.GetComponent<GamePlayUI>().buttons.Length > i)
                {
                    if(UIMagnager.GetComponent<GamePlayUI>().buttons[i] != null){
                        if(levelMagnager.GetComponent<TeleportTracker>().telea[i] == true) {
                            UIMagnager.GetComponent<GamePlayUI>().buttons[i].gameObject.SetActive(true);
                        }
                    }
                    //Debug.Log(levelMagnager.GetComponent<TeleportTracker>().telea[i]);
                    //UIMagnager.GetComponent<GamePlayUI>().buttons[i].gameObject.SetActive(true);
                }
        }
    }

}