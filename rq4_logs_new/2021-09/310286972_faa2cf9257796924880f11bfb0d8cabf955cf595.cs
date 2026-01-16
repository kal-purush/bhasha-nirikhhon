using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ExplosionSphere_Y : MonoBehaviour
{
    private float timer;
    [Range(0.1f, 30f)] public float targetScale;
    private Vector3 scale = new Vector3(1f, 1f, 1f);
    public float deleteTime;

    // Update is called once per frame
    void Update()
    {
        timer += Time.deltaTime;
        transform.localScale = Vector3.Lerp(Vector3.zero, scale * targetScale, timer / deleteTime);
        if (timer >= deleteTime) Destroy(gameObject);
    }
}