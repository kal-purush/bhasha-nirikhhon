using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Quest_Controller : MonoBehaviour
{
    public GameObject questPopup;
    GameObject createdQuest;
    public Sprite mailIcon;
    public Sprite newMailIcon;
    int timer = 0;

	// Use this for initialization
	void Start ()
    {
        timer = Random.Range(480, 916);
	}
	
	// Update is called once per frame
	void Update ()
    {
		if(timer > 0)
        {
            timer--;
        }
        else
        {
            if(Game_Manager.Instance.currentNumQuests < 3)
            {
                CreateQuest();
            }
        }
	}

    public void CreateQuest()
    {
        int questType = Random.Range(1, 4);

        switch(questType)
        {
            case 1:
                OnePlantTypeQuest();
                break;
            case 2:
                TwoPlantTypeQuest();
                break;
            case 3:
                ThreePlantTypeQuest();
                break;
            default:
                TwoPlantTypeQuest();
                break;
        }

        GetComponent<Image>().sprite = newMailIcon;
    }

    public void OnePlantTypeQuest()
    {
        createdQuest = Instantiate(questPopup);
        createdQuest.GetComponent<Quest_Popup>().type = 1;
        
        switch(Random.Range(1, 5))
        {
            case 1:
                createdQuest.GetComponent<Quest_Popup>().basicRequired = Random.Range(1, 4);
                break;
            case 2:
                createdQuest.GetComponent<Quest_Popup>().fireRequired = Random.Range(1, 4);
                break;
            case 3:
                createdQuest.GetComponent<Quest_Popup>().iceRequired = Random.Range(1, 4);
                break;
            case 4:
                createdQuest.GetComponent<Quest_Popup>().voidRequired = Random.Range(1, 4);
                break;
            default:
                createdQuest.GetComponent<Quest_Popup>().basicRequired = Random.Range(1, 4);
                break;
        }
    }

    public void TwoPlantTypeQuest()
    {

    }

    public void ThreePlantTypeQuest()
    {

    }
}