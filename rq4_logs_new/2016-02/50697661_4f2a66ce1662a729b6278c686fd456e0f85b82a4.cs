using UnityEngine;
using System.Collections;

public class OriginPoint : MonoBehaviour {

    void OnDrawGizmos()
    {
        Gizmos.color = Color.red;
        Vector3 corner1 = transform.position + new Vector3(0.5f * SpawnPoint.WIDTH, 0.5f * SpawnPoint.HEIGHT, 0);
        Vector3 corner2 = transform.position + new Vector3(0.5f * SpawnPoint.WIDTH, -0.5f * SpawnPoint.HEIGHT, 0);
        Vector3 corner3 = transform.position + new Vector3(-0.5f * SpawnPoint.WIDTH, -0.5f * SpawnPoint.HEIGHT, 0);
        Vector3 corner4 = transform.position + new Vector3(-0.5f * SpawnPoint.WIDTH, 0.5f * SpawnPoint.HEIGHT, 0);
        Gizmos.DrawLine(corner1, corner2);
        Gizmos.DrawLine(corner2, corner3);
        Gizmos.DrawLine(corner3, corner4);
        Gizmos.DrawLine(corner4, corner1);

    }
}