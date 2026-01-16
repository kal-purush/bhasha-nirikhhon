using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AutoDestroy : MonoBehaviour
{
    public float time;
    // Start is called before the first frame update
    void Start()
    {
        StartCoroutine(CoroutineWaitForSecond(time));
    }

    IEnumerator CoroutineWaitForSecond(float time)
    {
        yield return new WaitForSeconds(time);
        Destroy(gameObject);
    }
}