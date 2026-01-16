using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Quest_Controller : MonoBehaviour
{
    int questType = 0;
    int basicRequired = 0;
    int fireRequired = 0;
    int iceRequired = 0;
    int voidRequired = 0;

	// Use this for initialization
	void Start ()
    {
		
	}
	
	// Update is called once per frame
	void Update ()
    {
		
	}

    public void CreateQuest()
    {
        questType = Random.Range(1, 4);

        switch(questType)
        {
            case 1:
                break;
            case 2:
                break;
            case 3:
                break;
            default:
                break;
        }
    }
}