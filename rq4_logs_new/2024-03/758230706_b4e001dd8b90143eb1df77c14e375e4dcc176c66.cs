using UnityEngine;
using UnityEngine.Rendering.Universal; 

public class LightTrigger2D : MonoBehaviour
{
    public Light2D targetLight;

    private void OnTriggerEnter2D(Collider2D other)
    {
        if (other.CompareTag("Player_01"))
        {
            Debug.Log("Player 1 entered trigger. Turning on light.");
            targetLight.intensity = 1f;
        }
        else if (other.CompareTag("Player_02"))
        {
            Debug.Log("Player 2 entered trigger. Turning on light.");
            targetLight.intensity = 1f;
        }
    }

}