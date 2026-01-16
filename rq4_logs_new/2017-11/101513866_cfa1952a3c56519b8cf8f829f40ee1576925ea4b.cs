using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ObjectiveData : MonoBehaviour
{
    public Button completeQuestButton;
    public Sprite basicPlant;
    public Sprite firePlant;
    public Sprite icePlant;
    public Sprite voidPlant;
    public GameObject plant1;
    public GameObject plant2;
    public GameObject plant3;
    public GameObject plant4;
    public GameObject plant1Grown;
    public GameObject plant2Grown;
    public GameObject plant3Grown;
    public GameObject plant4Grown;
    public GameObject Objective1;
    public GameObject Objective2;
    public GameObject Objective3;
    public int type = 2;
    public int ObjectiveNum = 0;
    public int basicRequired = 0;
    public int fireRequired = 0;
    public int iceRequired = 0;
    public int voidRequired = 0;
    int questReward = 0;

    // Use this for initialization
    void Start ()
    {
        Objective1 = Game_Manager.Instance.Objective1;
        Objective2 = Game_Manager.Instance.Objective2;
        Objective3 = Game_Manager.Instance.Objective3;
    }
	
	// Update is called once per frame
	void Update ()
    {
        switch (ObjectiveNum)
        {
            case 1:
                UpdateObjectives(Objective1);
                break;
            case 2:
                UpdateObjectives(Objective2);
                break;
            case 3:
                UpdateObjectives(Objective3);
                break;
        }

        if ((Game_Manager.Instance.basicPlantsHarvested >= basicRequired) && (Game_Manager.Instance.firePlantsHarvested >= fireRequired) && (Game_Manager.Instance.icePlantsHarvested >= iceRequired) && (Game_Manager.Instance.voidPlantsHarvested >= voidRequired))
        {
            completeQuestButton.interactable = true;
        }

        if((basicRequired <= 0) && (fireRequired <= 0) && (iceRequired <= 0) && (voidRequired <= 0))
        {
            completeQuestButton.interactable = false;
        }
    }

    public void CompleteQuest()
    {
        if(ObjectiveNum == 1)
        {
            ObjectiveNum = 3;
            Game_Manager.Instance.Objective2.GetComponent<ObjectiveData>().ObjectiveNum = 1;
            Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().ObjectiveNum = 2;
        }
        else if(ObjectiveNum == 2)
        {
            ObjectiveNum = 3;
            Game_Manager.Instance.Objective1.GetComponent<ObjectiveData>().ObjectiveNum = 1;
            Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().ObjectiveNum = 2;
        }
        else
        {
            ObjectiveNum = 3;
            Game_Manager.Instance.Objective1.GetComponent<ObjectiveData>().ObjectiveNum = 1;
            Game_Manager.Instance.Objective2.GetComponent<ObjectiveData>().ObjectiveNum = 2;
        }

        plant1.SetActive(false);
        plant2.SetActive(false);
        plant3.SetActive(false);
        plant4.SetActive(false);
        plant1Grown.SetActive(false);
        plant2Grown.SetActive(false);
        plant3Grown.SetActive(false);
        plant4Grown.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().plant1.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().plant2.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().plant3.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().plant4.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().plant1Grown.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().plant2Grown.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().plant3Grown.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().plant4Grown.SetActive(false);
        Game_Manager.Instance.Objective3.GetComponent<ObjectiveData>().completeQuestButton.interactable = false;

        completeQuestButton.interactable = false;

        if (basicRequired > 0)
        {
            questReward += 25 * basicRequired;
            Game_Manager.Instance.basicPlantsHarvested--;
        }

        if (fireRequired > 0)
        {
            questReward += 50 * fireRequired;
            Game_Manager.Instance.firePlantsHarvested--;
        }

        if (iceRequired > 0)
        {
            questReward += 50 * iceRequired;
            Game_Manager.Instance.icePlantsHarvested--;
        }

        if (voidRequired > 0)
        {
            questReward += 50 * voidRequired;
            Game_Manager.Instance.voidPlantsHarvested--;
        }

        switch(type)
        {
            case 1:
                questReward += 100;
                break;
            case 2:
                questReward += 200;
                break;
            case 3:
                questReward += 300;
                break;
            default:
                questReward += 200;
                break;
        }

        Game_Manager.Instance.money += questReward;
        questReward = 0;
        Game_Manager.Instance.currentNumQuests--;
    }

    public void UpdateObjectives(GameObject objective)
    {
        objective.SetActive(true);

        objective.GetComponent<ObjectiveData>().type = type;
        objective.GetComponent<ObjectiveData>().basicRequired = basicRequired;
        objective.GetComponent<ObjectiveData>().fireRequired = fireRequired;
        objective.GetComponent<ObjectiveData>().iceRequired = iceRequired;
        objective.GetComponent<ObjectiveData>().voidRequired = voidRequired;

        #region Update Basic

        if (basicRequired > 0)
        {
            objective.GetComponent<ObjectiveData>().plant1.SetActive(true);
            objective.GetComponent<ObjectiveData>().plant1Grown.SetActive(true);
            objective.GetComponent<ObjectiveData>().plant1.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().basicPlant;
            objective.GetComponent<ObjectiveData>().plant1Grown.GetComponent<Text>().text = "0 / " + basicRequired;
        }

        #endregion

        #region Update Fire

        if (fireRequired > 0)
        {
            if (basicRequired > 0)
            {
                objective.GetComponent<ObjectiveData>().plant2.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant2Grown.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant2.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().firePlant;
                objective.GetComponent<ObjectiveData>().plant2Grown.GetComponent<Text>().text = "0 / " + fireRequired;
            }
            else
            {
                objective.GetComponent<ObjectiveData>().plant1.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant1Grown.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant1.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().firePlant;
                objective.GetComponent<ObjectiveData>().plant1Grown.GetComponent<Text>().text = "0 / " + fireRequired;
            }
        }

        #endregion

        #region Update Ice

        if (iceRequired > 0)
        {
            if (fireRequired > 0)
            {
                if (basicRequired > 0)
                {
                    objective.GetComponent<ObjectiveData>().plant3.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant3Grown.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant3.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().icePlant;
                    objective.GetComponent<ObjectiveData>().plant3Grown.GetComponent<Text>().text = "0 / " + iceRequired;
                }
                else
                {
                    objective.GetComponent<ObjectiveData>().plant2.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant2Grown.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant2.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().icePlant;
                    objective.GetComponent<ObjectiveData>().plant2Grown.GetComponent<Text>().text = "0 / " + iceRequired;
                }
            }
            else if (basicRequired > 0)
            {
                objective.GetComponent<ObjectiveData>().plant2.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant2Grown.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant2.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().icePlant;
                objective.GetComponent<ObjectiveData>().plant2Grown.GetComponent<Text>().text = "0 / " + iceRequired;
            }
            else
            {
                objective.GetComponent<ObjectiveData>().plant1.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant1Grown.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant1.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().icePlant;
                objective.GetComponent<ObjectiveData>().plant1Grown.GetComponent<Text>().text = "0 / " + iceRequired;
            }
        }

        #endregion

        #region Update Void

        if (voidRequired > 0)
        {
            if (iceRequired > 0)
            {
                if (fireRequired > 0)
                {
                    if (basicRequired > 0)
                    {
                        objective.GetComponent<ObjectiveData>().plant4.SetActive(true);
                        objective.GetComponent<ObjectiveData>().plant4Grown.SetActive(true);
                        objective.GetComponent<ObjectiveData>().plant4.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().voidPlant;
                        objective.GetComponent<ObjectiveData>().plant4Grown.GetComponent<Text>().text = "0 / " + voidRequired;
                    }
                    else
                    {
                        objective.GetComponent<ObjectiveData>().plant3.SetActive(true);
                        objective.GetComponent<ObjectiveData>().plant3Grown.SetActive(true);
                        objective.GetComponent<ObjectiveData>().plant3.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().voidPlant;
                        objective.GetComponent<ObjectiveData>().plant3Grown.GetComponent<Text>().text = "0 / " + voidRequired;
                    }
                }
                else if (basicRequired > 0)
                {
                    objective.GetComponent<ObjectiveData>().plant3.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant3Grown.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant3.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().voidPlant;
                    objective.GetComponent<ObjectiveData>().plant3Grown.GetComponent<Text>().text = "0 / " + voidRequired;
                }
                else
                {
                    objective.GetComponent<ObjectiveData>().plant2.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant2Grown.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant2.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().voidPlant;
                    objective.GetComponent<ObjectiveData>().plant2Grown.GetComponent<Text>().text = "0 / " + voidRequired;
                }
            }
            else if (fireRequired > 0)
            {
                if (basicRequired > 0)
                {
                    objective.GetComponent<ObjectiveData>().plant3.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant3Grown.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant3.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().voidPlant;
                    objective.GetComponent<ObjectiveData>().plant3Grown.GetComponent<Text>().text = "0 / " + voidRequired;
                }
                else
                {
                    objective.GetComponent<ObjectiveData>().plant2.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant2Grown.SetActive(true);
                    objective.GetComponent<ObjectiveData>().plant2.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().voidPlant;
                    objective.GetComponent<ObjectiveData>().plant2Grown.GetComponent<Text>().text = "0 / " + voidRequired;
                }
            }
            else if (basicRequired > 0)
            {
                objective.GetComponent<ObjectiveData>().plant2.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant2Grown.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant2.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().voidPlant;
                objective.GetComponent<ObjectiveData>().plant2Grown.GetComponent<Text>().text = "0 / " + voidRequired;
            }
            else
            {
                objective.GetComponent<ObjectiveData>().plant1.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant1Grown.SetActive(true);
                objective.GetComponent<ObjectiveData>().plant1.GetComponent<SpriteRenderer>().sprite = objective.GetComponent<ObjectiveData>().voidPlant;
                objective.GetComponent<ObjectiveData>().plant1Grown.GetComponent<Text>().text = "0 / " + voidRequired;
            }
        }

        #endregion
    }
}