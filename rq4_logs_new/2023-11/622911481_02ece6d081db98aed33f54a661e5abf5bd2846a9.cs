using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/* TODO:
- Add Sprites for the rock
- Add Dialogue after impact
- Add Quest
- Add Other Images for the Quest
*/

public class ShipHandler : MonoBehaviour
{
    [SerializeField] private float spawnRockTimer = 5f;
    [SerializeField] private GameObject Rock;
    [SerializeField] private GameObject ImpactPoint;
    [SerializeField] private int x, y;
    [SerializeField] private CameraShake cameraShake;

    private void Start()
    {
        StartCoroutine(SpawnRock());
    }

    private IEnumerator SpawnRock()
    {
        yield return new WaitForSeconds(spawnRockTimer);
        Rock.SetActive(true);
        MoveRockToShip();
    }

    private void MoveRockToShip()
    {
        // Slowly move the rock to the ship
        StartCoroutine(MoveRock());
    }

    private IEnumerator MoveRock()
    {
        // Morph the rock to the ship till it is at this gameobjects transformposition
        float morphTime = 1f;
        float elapsedTime = 0f;
        Vector2 startPosition = Rock.transform.position;

        while (elapsedTime < morphTime)
        {
            Rock.transform.position = Vector2.Lerp(startPosition, (Vector2)ImpactPoint.transform.position, elapsedTime / morphTime);
            elapsedTime += Time.deltaTime;
            yield return null;
        }
        cameraShake.ShakeCamera();
    }
}