using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SpawnSpider : MonoBehaviour
{
    [SerializeField] private GameObject SpiderPrefab;
    [SerializeField] private GameObject SpiderWarning;
    private bool CheckForValidSpawn;

    private Transform betterEnemySpawner;

    void Start()
    {
        betterEnemySpawner = GameObject.FindGameObjectWithTag("betterEnemySpawner").GetComponent<Transform>();
        CheckForValidSpawn = true;
        StartCoroutine(ValidSpawn());
        StartCoroutine(SpawnSpiderDelay());
    }

    private IEnumerator SpawnSpiderDelay()
    {
        yield return new WaitForSeconds(1f);
        Instantiate(SpiderPrefab, transform.position, Quaternion.identity);
        yield return null;
    }

    private void OnTriggerEnter2D(Collider2D collider)
    {
        bool UndesirableSpawn = collider.gameObject.tag == "MapCollider" || collider.gameObject.tag == "chest" || collider.gameObject.tag == "Player";
        if (CheckForValidSpawn == true && UndesirableSpawn == true)
        {
            Instantiate(SpiderWarning, new Vector3(betterEnemySpawner.position.x + Random.Range(-3f, 3f), betterEnemySpawner.position.y + Random.Range(-3f, 3f), 0), Quaternion.identity);
            Destroy(gameObject);
        }  
    }

    private IEnumerator ValidSpawn()
    {
        yield return new WaitForSeconds(0.1f);
        CheckForValidSpawn = false;
        yield return null;
    }

}