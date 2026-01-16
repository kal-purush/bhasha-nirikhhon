using UnityEngine;

public class ShakeAnimationBehavior : MonoBehaviour
{
    private Vector3 originalPosition;
    private float progress = 0;

    public float amplitude = 0.2f;
    public float speed = 0.1f;

    public void Play()
    {
        progress = speed;
    }

    public void Start()
    {
        originalPosition = transform.position;
    }

    public void Update()
    {
        if (progress == 0)
        {
            return;
        }

        var z = Mathf.Sin(progress) * amplitude;
        transform.position = originalPosition + new Vector3(0, 0, z);

        progress += speed;

        if (progress >= 1)
        {
            progress = 0;
        }
    }
}