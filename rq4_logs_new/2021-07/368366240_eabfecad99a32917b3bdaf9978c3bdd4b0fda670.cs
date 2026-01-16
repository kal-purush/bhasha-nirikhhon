using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class EffectGenerater : MonoBehaviour
{
    public GameObject effectSprite;

    public void GenerateEffect(Vector3 generatePosition, Quaternion generateRotate)
    {
        GameObject effectObject = Instantiate(effectSprite);
        effectObject.transform.position = generatePosition;
        effectObject.transform.rotation = generateRotate;
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        Vector3 tempTransform = collision.transform.localPosition;
        Quaternion tempRotate = Quaternion.FromToRotation(new Vector3(0,1,0), collision.gameObject.transform.localPosition);
        GenerateEffect(tempTransform, tempRotate);
    }
}