using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using UnityEngine;
using UnityEngine.UI;



public class Game : MonoBehaviour
{
    [SerializeField] GameObject menu;
    [SerializeField] Button resumeButton;
    [SerializeField] Button newGameButton;
    public PlayerScript playerScript;
    public GameObject[] savePoints;
    private bool isPaused;

    private void Awake()
    {
        isPaused = false;
        Pause();
    }

    void Start()
    {
        Cursor.visible = false;
    }

    void Update()
    {
        if (Input.GetButtonDown("Start"))
        {
            Debug.Log(isPaused);
            if (isPaused)
            {
                Unpause();
            }
            else
            {
                Pause();
            }
        }
    }

    private Save CreateSaveGameObject()
    {
        Save save = new Save();

        // foreach (GameObject enemy in liveEnemies)
        // {
        //     save.livingTargetPositions.Add(enemy.transform.position);
        // }

        save.savePointIndex = 1;

        return save;
    }

    public void NewGame()
    {
        ClearEnemies();
        ResetPlayerPosition();
        Unpause();
    }

    public void SaveGame()
    {
        Save save = CreateSaveGameObject();
        BinaryFormatter bf = new BinaryFormatter();
        FileStream file = File.Create(Application.persistentDataPath + "/gamesave.save");
        bf.Serialize(file, save);
        file.Close();

        Debug.Log("Game Saved at: " + Application.persistentDataPath + "/gamesave.save");
    }

    public void LoadGame()
    {
        if (File.Exists(Application.persistentDataPath + "/gamesave.save"))
        {
            ClearEnemies();
            ResetPlayerPosition();

            BinaryFormatter bf = new BinaryFormatter();
            FileStream file = File.Open(Application.persistentDataPath + "/gamesave.save", FileMode.Open);
            Save save = (Save)bf.Deserialize(file);
            file.Close();
            playerScript.transform.position = savePoints[save.savePointIndex].transform.position;
            Debug.Log("Game Loaded");
            Unpause();
        }
        else
        {
            Debug.Log("No Game Saved!");
        }
    }

    public void Pause()
    {
        menu.SetActive(true);
        newGameButton.Select();
        resumeButton.Select();
        Time.timeScale = 0;
        isPaused = true;
    }

    public void Unpause()
    {
        menu.SetActive(false);
        // Cursor.visible = false;
        Time.timeScale = 1;
        isPaused = false;
    }

    public bool IsGamePaused()
    {
        return isPaused;
    }

    private void ClearEnemies()
    {
        foreach(GameObject enemy in GameObject.FindGameObjectsWithTag("PigTag"))
        {
            Destroy(enemy);
        }
    }

    private void ResetPlayerPosition()
    {
        GameObject.FindGameObjectWithTag("Player").transform.position = playerScript.spawnPoint;
    }


}