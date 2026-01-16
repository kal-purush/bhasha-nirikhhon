using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Script_CheckpointsManager : MonoBehaviour
{
    //TODO Remove Serialize attribute once debug is completed.
    [SerializeField] List<GameObject> checkpoints;

    private int currentCheckpoint;

    private Script_UIController m_Script_UIController;
    private Script_MenuController m_Script_MenuController;
    private Script_Countdown m_Script_Countdown;

    void Awake()
    {
        checkpoints.Clear();// Prevents adding checkpoints more than once in case they were added in edit mode

        currentCheckpoint = 0;

        var checkpointList = GameObject.FindGameObjectsWithTag("Checkpoint");

        foreach (var checkpoint in checkpointList)
        {
            checkpoint.SetActive(false);
            checkpoints.Add(checkpoint);
        }

        m_Script_UIController = GameObject.FindWithTag("UI").GetComponent<Script_UIController>();
        m_Script_MenuController = GameObject.FindWithTag("Menu").GetComponent<Script_MenuController>();
        m_Script_Countdown = GameObject.FindWithTag("RootGame").GetComponentInChildren<Script_Countdown>();
    }
    void Start()
    {
        if (checkpoints.Count > 0)
        {
            checkpoints[currentCheckpoint].SetActive(true);
        }
        else
        {
            Debug.LogError("There are no checkpoints in the scene. Please, make sure there is always at least one checkpoint in the scene");
        }
    }

    public void CheckAndGoNext()
    {
        checkpoints[currentCheckpoint].SetActive(false);

        currentCheckpoint++;

        if (currentCheckpoint >= checkpoints.Count)
        {
            m_Script_MenuController.EndGame(Script_MenuController.EndOption.Win);
        }
        else
        {
            string rewardMessage;
            if(checkpoints.Count - currentCheckpoint == 1)
            {
                rewardMessage = "Almost there!! Only one place to go!";
            }
            else if (checkpoints.Count - currentCheckpoint <= 3)
            {
                rewardMessage = "Keep going!! You are about to make it!";
            }
            else
            {
                rewardMessage = "Great!! You can do it!";
            }

            m_Script_UIController.SetTextCheckpointReward(rewardMessage);
            m_Script_Countdown.IncreaseTime();
            checkpoints[currentCheckpoint].SetActive(true);
        }
    }
}