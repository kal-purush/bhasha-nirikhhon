using System.Collections;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class TimeCount:MonoBehaviour
{
    public GameObject text;
    public int TotalTime = 60;

    void Start()
    {

        StartCoroutine(CountDown());

    }

    IEnumerator CountDown()
    {
        while (TotalTime >= 0)
        {
            text.GetComponent<Text>().text = TotalTime.ToString();
            yield return new WaitForSeconds(1);
            TotalTime--;

        }
    }
}