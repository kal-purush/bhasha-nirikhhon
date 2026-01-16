using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class StartUp : MonoBehaviour
{
    [SerializeField] private RawImage blackOut; // Change this to RawImage type
    [SerializeField] private float timer = 3; // Change this to RawImage type

    // Start is called before the first frame update
    void Start()
    {
        StartCoroutine(FadeUpCoroutine());

    }

    IEnumerator FadeUpCoroutine()
    {

        Color color = blackOut.color;
        while (color.a > 0)
        {
            color.a -= Time.deltaTime * 0.5f; // Adjust the speed of fade
            blackOut.color = color;
            yield return null; // Yield to wait for the next frame
        }

        while (timer > 0)
        {
            timer -= Time.deltaTime * 0.5f; // Adjust the speed of fade
            yield return null; // Yield to wait for the next frame
        }
        SceneManager.LoadScene(4);
        yield break;
    }

}