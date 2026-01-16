using UnityEngine;

public class BatController : MonoBehaviour
{
    public float flySpeed = 2f; 
    public float waveFrequency = 3.5f; 
    public float waveMagnitude = 2f; 
    public float destroyTime = 10f; 

    private Vector3 startPosition;
    private float startTime;

    private void Start()
    {
        startPosition = transform.position;
        startTime = Time.time;

    
        Destroy(gameObject, destroyTime);
    }

    private void Update()
    {
        float timeSinceStart = Time.time - startTime;

   
        transform.position = startPosition + new Vector3(
            Mathf.Sin(timeSinceStart * waveFrequency) * waveMagnitude, 
            timeSinceStart * flySpeed, 
            0
        );
    }
}