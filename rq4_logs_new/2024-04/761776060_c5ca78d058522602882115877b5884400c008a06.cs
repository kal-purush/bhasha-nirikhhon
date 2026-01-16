using System.Collections;
using UnityEngine;
using UnityEngine.UI;

public class FadeIn : MonoBehaviour
{
    [SerializeField] private RawImage blackOut; // Change this to RawImage type

    void Start()
    {
        StartCoroutine(FadeInCoroutine());
    }

    IEnumerator FadeInCoroutine()
    {
        Color color = blackOut.color;
        while (color.a > 0)
        {
            color.a -= Time.deltaTime * 0.5f; // Adjust the speed of fade
            blackOut.color = color;
            yield return null;
        }
        blackOut.enabled = false;
    }
}