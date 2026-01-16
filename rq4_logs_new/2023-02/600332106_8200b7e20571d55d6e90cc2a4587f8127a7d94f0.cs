using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using TDLN.CameraControllers;

public class GameManager : MonoBehaviour
{
    [Header("References")]
    public Transform cam;

    [Header("Stats")]
    public int Health = 3;
    public int Points = 0;

    [Header("UI")]
    public Text HealthName;
    public Text PointsName;
    public Text HealthName2;
    public Text PointsName2;

    public static GameManager instance;

    private void Awake()
    {
        instance = this;
    }
    void Start()
    {
        HealthName.text = "Health: " + Health;
        HealthName2.text = "Health: " + Health;

        PointsName.text = "Score: " + Points;
        PointsName2.text = "Score: " + Points;
    }
    // Start is called before the first frame update
    public void Healthdecrease()
    {
        Health--;
        HealthName.text = "Health: " + Health;
        HealthName2.text = "Health: " + Health;
        if (Health <= 0)
        {
            //Gameover

            OnHealthReachedZero();
        }
    }

    [ContextMenu("OnHealthReachedZero")]
    void OnHealthReachedZero()
    {
        cam.GetComponent<CameraOrbit>().enabled = false;
        cam.parent = null;
        PlayerController.instance.Shatter();
    }

    public void EndGame()
    {
        SetInt("lastscore", Points);
        SceneManager.LoadScene("gameover", LoadSceneMode.Single);
    }

    // Update is called once per frame
    public void Scoreincrease()
    {
        Points++;
        PointsName.text = "Score: " + Points;
        PointsName2.text = "Score: " + Points;
    }

    public void SetInt(string KeyName, int Value)
    {
        PlayerPrefs.SetInt(KeyName, Value);
    }

    public int Getint(string KeyName)
    {
        return PlayerPrefs.GetInt(KeyName);
    }

    public void UpdateText()
    {
        HealthName.text = "Health: " + Health;
        HealthName2.text = "Health: " + Health;

        PointsName.text = "Score: " + Points;
        PointsName2.text = "Score: " + Points;
    }
}