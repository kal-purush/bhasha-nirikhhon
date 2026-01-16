using UnityEngine;
using System.Collections;


public class QuestManager : MonoBehaviour
{
    public GameObject[] quests;
    int questIndex = 0;
    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            Debug.Log(quests[questIndex]); //Prints current value to console.
            if (questIndex >= quests.Length - 1)
            {
                questIndex = 0;
            }
            else
            {
                questIndex += 1;
            }
        }
    }
}
