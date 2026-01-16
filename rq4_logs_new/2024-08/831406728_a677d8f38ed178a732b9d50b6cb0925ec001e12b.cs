using System.Collections;
using System.Collections.Generic;
using UnityEditor;
using UnityEngine;
using UnityEngine.Timeline;

public class EnemyIA : MonoBehaviour
{
    public float speed = 4.0f;
    public GameObject prefabEnemy;

    // Start is called before the first frame update
    void Start()
    {
        transform.position = new Vector3(0, 6, 0);
    }

    // Update is called once per frame
    void Update()
    {
        transform.Translate(Vector3.down * speed * Time.deltaTime);
        SpawnEnemy();
    }

    private void SpawnEnemy()
    {
        if(transform.position.y < -10)
        {
            transform.position = new Vector3(Random.Range(-8f, 8f), 8f, 0);
        }
    }
}