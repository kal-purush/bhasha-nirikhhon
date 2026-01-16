using UnityEngine;

public class ClosestEnemy : MonoBehaviour
{
    public GameObject AimedEnemy { get; set; }

    private void Update()
    {
        if (Camera.main == null) return;
        Ray cameraRay = Camera.main.ScreenPointToRay(transform.position);
        if (Physics.Raycast(cameraRay, out RaycastHit hitInfo))
        {
            var hitObject = hitInfo.transform.gameObject;
            if (hitInfo.transform.CompareTag("Enemy") && AimedEnemy != hitObject)
            {
                AimedEnemy = hitObject;
            }
        }
    }
}