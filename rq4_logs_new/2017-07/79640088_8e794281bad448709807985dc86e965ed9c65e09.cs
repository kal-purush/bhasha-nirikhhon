using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class objectShake : MonoBehaviour {

    Vector3 startTransform;
    public float time = 3;
    public float strength = 0.75f;

    void Awake() {
        startTransform = transform.localPosition;
    }

    public void shake() {
        StartCoroutine(screenshake(time, strength));
    }

    IEnumerator screenshake(float t, float strength) {
        float z = transform.localPosition.z;
        while (t > 0) {
            t -= Time.deltaTime * 10;

            transform.localPosition = new Vector2(startTransform.x, startTransform.y) + Random.insideUnitCircle * strength / 8;
            transform.localPosition += new Vector3(0, 0, z);
            yield return null;
        }
        transform.localPosition = startTransform;
    }
}