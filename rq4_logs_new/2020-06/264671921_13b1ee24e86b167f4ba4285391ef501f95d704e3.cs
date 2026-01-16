using UnityEngine;

public class Bobbing : MonoBehaviour
{
    float floatY;
    float originalY;
    float offset;

    public float floatStrength = 1;

    void Start()
    {
        offset = Random.Range(0.5f, 1.5f);
        originalY = transform.position.y;
    }

    void Update()
    {
        Vector3 pos = transform.position;
        floatY = pos.y;
        floatY = originalY + (Mathf.Sin(Time.time * offset) * floatStrength);
        transform.position = new Vector3(pos.x, floatY, pos.z);
    }
}