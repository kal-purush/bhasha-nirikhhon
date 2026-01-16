using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CanvasShaker : MonoBehaviour{

    public static CanvasShaker instance;

    public Transform panel;
    Vector3 originalPosition; 

    private void Awake() {
        instance = instance ? instance : this;
        DontDestroyOnLoad(instance);

        panel = panel ? panel : transform;
        originalPosition = panel.transform.position;
    }

    public void Shake(float magnitude, float duration){
        StartCoroutine(ShakeCoroutine(magnitude, duration));
    }

    IEnumerator ShakeCoroutine(float magnitude, float duration){



        float timeElapsed = 0f;
        while(timeElapsed < duration){

            panel.transform.position = originalPosition
                + Vector3.Normalize(new Vector3(Random.Range(-1f, 1f), Random.Range(-1f, 1f), Random.Range(-1f, 1f)))
                * Random.Range(-magnitude, magnitude) * (duration - timeElapsed);

            timeElapsed += Time.deltaTime;
            yield return null;
        }

        panel.transform.position = originalPosition;
    }
}