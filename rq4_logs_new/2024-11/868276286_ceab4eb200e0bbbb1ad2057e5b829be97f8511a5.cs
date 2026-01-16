using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
using UnityEngine.SceneManagement;



public class IntroTextSequence : MonoBehaviour
{

    bool revert = false;
    bool pauseDone = false;

    [SerializeField] float upTime = 1f;
    [SerializeField] float waitOn = 2f;
    [SerializeField] float waitOff = 1f;
    int i = 0;

    [SerializeField] TextMeshProUGUI[] textArray;
    void Start()
    {

    }

    void Update()
    {
        if (i < textArray.Length && !pauseDone)
        {
            runPlay(textArray[i]);
        }
        if (i >= textArray.Length)
        {
            SceneManager.LoadScene("TownSpan");
        }
    }

    void runPlay(TextMeshProUGUI currText)
    {

        if (!revert && currText.color.a <= 1f)
        {
            currText.color = new Color(currText.color.r, currText.color.g, currText.color.b, currText.color.a + (Time.deltaTime / upTime));

        }
        else if (!revert && currText.color.a > 1f)
        {
            revert = !revert;
            pauseDone = true;
            StartCoroutine(waitTime(waitOn));



        }
        else if (revert && currText.color.a >= 0f)
        {
            currText.color = new Color(currText.color.r, currText.color.g, currText.color.b, currText.color.a - (Time.deltaTime / upTime));

        }
        else
        {
            i++;
            revert = !revert;
            pauseDone = true;
            StartCoroutine(waitTime(waitOff));

        }
    }

    IEnumerator waitTime(float waitingTime)
    {
        Debug.Log("waiting");
        yield return new WaitForSeconds(waitingTime);
        pauseDone = false;
    }
}
