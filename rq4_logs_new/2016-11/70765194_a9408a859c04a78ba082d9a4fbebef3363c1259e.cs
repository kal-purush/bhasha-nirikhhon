using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class MoveHeart : MonoBehaviour
{
    float activeTime = 2.5f;

    [SerializeField]
    public Vector3 position;

    [SerializeField]
    public Vector3 speed;

    public void Make()
    {
        position = GetComponent<RectTransform>().position;
        Debug.Log(position);
        GetComponent<RectTransform>().position = position;
        speed = new Vector3(Random.Range(-20.0f, 20.0f), Random.Range(5.0f,8.0f), 0.0f);
    }

    void Update()
    {
        position += new Vector3(speed.x * Time.deltaTime * Mathf.Sin(activeTime * 5), speed.y * Time.deltaTime, 0.0f);
        this.gameObject.GetComponent<RectTransform>().position = position;

        activeTime -= Time.deltaTime;

        if (activeTime > 0) return;
        Destroy(gameObject);
    }
}