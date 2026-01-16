using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class EndMotionB : MonoBehaviour
{
    public GameObject black;
    Image rend;
    bool blackTrig;
    public float darkSec;
    Color color;

    public string nextStageName;
    [SerializeField]
    float waitTime;

    // Start is called before the first frame update
    void Start()
    {
        blackTrig = false;
        rend = black.GetComponent<Image>();
    }

    // Update is called once per frame
    void Update()
    {
        if (blackTrig == true && color.a <= 1.0f)
        {
            color = rend.color;
            color.a += Time.deltaTime / (darkSec);
            rend.color = color;
        }
        if (color.a > 1.0f)
        {
            Invoke("ChangeSceneEnd", waitTime);
        }
    }

    public void TriggerBlack()
    {
        blackTrig = true;
    }

    void ChangeSceneEnd()
    {
        SceneManager.LoadScene(nextStageName);
    }
}