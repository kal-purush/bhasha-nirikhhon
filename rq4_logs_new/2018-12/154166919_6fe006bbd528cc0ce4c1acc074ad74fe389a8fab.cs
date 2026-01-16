using System.Collections;
using System.Collections.Generic;
using UnityEngine;


public class Curtain : MonoBehaviour
{
    public GameObject[] ThingsToShow = new GameObject[7];
    public GameObject Curtain_Open;
    public GameObject Curtain_Close;
    public enum State { Open, Close };
    public State StartStates;
    private bool Open;
    private bool start;

    private void Switch(bool s)
    {
        Curtain_Close.SetActive(!s);
        Curtain_Open.SetActive(s);
        if (!start)
        {
            if (s)
                Curtain_Open.GetComponent<AudioSource>().Play();
            else
                Curtain_Close.GetComponent<AudioSource>().Play();
        }
        ShowThings(s);
        
    }
    private void ShowThings(bool s)
    {
        if (!s)
            for (int i = 0; i < ThingsToShow.Length; i++)
                ThingsToShow[i].SetActive(false);
        else
        { 
            int i = Random.Range(0, 9);
            Debug.Log("i= " + i);
            if (i <= 2)//0.1.2bird
                ThingsToShow[0].SetActive(true);
            else if (i == 3 || i == 4)// Ghost, GameOver
            {
                ThingsToShow[1].SetActive(true);
                GameManager.gm.UnlockAchievement(1);
                Invoke("GameOver", 2f);
            }
            else 
                ThingsToShow[i - 3].SetActive(true);

        }
    }
    private void GameOver()
    {
        GameManager.gm.GameFaild();
    }
    // Use this for initialization
    void Start()
    {
        start = true;
        if (StartStates == State.Open)
            Open = true;
        else
            Open = false;
        Switch(Open);
    }

    private void OnMouseDown()
    {
        start = false;
        Open = !Open;
        Switch(Open);
       
    }
    // Update is called once per frame
    void Update()
    {

    }

}
