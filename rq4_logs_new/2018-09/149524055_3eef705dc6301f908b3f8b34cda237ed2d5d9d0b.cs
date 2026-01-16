using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class UITitleManager : MonoBehaviour
{
    GameObject imageObject;
    GameObject maskObject;
    GameObject textObject;
    bool alphaUp = false;
    bool isSceneEnd = false;

    // Use this for initialization
    void Start()
    {
        imageObject = GameObject.Find("ImageTitle");
        maskObject = GameObject.Find("ImageMask");
        textObject = GameObject.Find("TextTitle");
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.Mouse0))
        {
            StartCoroutine("SceneChange");
        }

        // update text alpha
        if (!isSceneEnd)
        {
            Color color = textObject.GetComponent<Text>().color;
            if (alphaUp)
            {
                color.a += 0.01f;
                if (color.a >= 1.0f)
                {
                    color.a = 1.0f;
                    alphaUp = false;
                }
            }
            else
            {
                color.a -= 0.01f;
                if (color.a <= 0.25f)
                {
                    color.a = 0.25f;
                    alphaUp = true;
                }
            }
            textObject.GetComponent<Text>().color = color;
        }
        else
        {
            Color color = imageObject.GetComponent<Image>().color;
            color.a -= 0.1f;
            if (color.a <= 0)
            {
                color.a = 0;
            }
            imageObject.GetComponent<Image>().color = color;
            color = maskObject.GetComponent<Image>().color;
            color.a -= 0.1f;
            if (color.a <= 0)
            {
                color.a = 0;
            }
            maskObject.GetComponent<Image>().color = color;
            color = textObject.GetComponent<Text>().color;
            color.a -= 0.1f;
            if (color.a <= 0)
            {
                color.a = 0;
            }
            textObject.GetComponent<Text>().color = color;
        }
    }

    IEnumerator SceneChange()
    {
        isSceneEnd = true;
        yield return new WaitForSeconds(2.0f);
        SceneManager.LoadScene("GameScene");
    }
}