using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FoodSpawner : MonoBehaviour
{
    public GameObject foodPrefab;
    public Transform borderTop;
    public Transform borderBottom;
    public Transform borderLeft;
    public Transform borderRight;


    void Start()
    {
        Spawn();
    }
    public void Spawn()
    {
        List<Vector2> spawn_locations = new List<Vector2>();
        for (int x = -120; x < 120; x = x + 10)
        {
            for (int y = -100; y < 100; y = y + 10)
            {
                if (Physics2D.OverlapCircle(new Vector2(x, y), 0.5f) == false)
                {
                    spawn_locations.Add(new Vector2(x, y));
                }
            }
        }
        Vector2 coords = spawn_locations[Random.Range(0, spawn_locations.Count)];
        Instantiate(foodPrefab, new Vector3(coords.x, coords.y, 0), Quaternion.identity);
    }
}