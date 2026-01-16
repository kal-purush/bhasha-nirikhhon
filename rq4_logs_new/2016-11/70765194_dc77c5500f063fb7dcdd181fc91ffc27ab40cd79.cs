using UnityEngine;
using System.Collections;
using System;

public class CoinFaller : MonoBehaviour
{
    private Easing easing = null;
    private float fallTime = 0.0f;
    public float fallTakeTime = 0.5f;

    void Start()
    {
        easing = GetComponent<Easing>();
    
        transform.localPosition =
            new Vector3(
                UnityEngine.Random.Range(-10.0f, 10.0f),
                300.0f,
                0.0f);
        transform.localScale = Vector3.one;

        StartCoroutine(UpdateCoin());
    }

    private IEnumerator UpdateCoin()
    {
        while (true)
        {
            fallTime = Mathf.Min(1.0f, fallTime + Time.deltaTime / fallTakeTime);
            transform.localPosition =
                new Vector3(
                transform.localPosition.x,
                easing.Linear(fallTime, 300.0f, 90.0f),
                transform.localPosition.z);
            if (fallTime == 1.0f)
                Destroy(this.gameObject);

            yield return null;
        }
    }
}