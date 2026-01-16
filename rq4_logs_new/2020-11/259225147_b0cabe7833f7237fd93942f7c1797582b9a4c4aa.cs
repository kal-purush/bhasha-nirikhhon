using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyPop : MonoBehaviour
{
    Transform playerTransform;
    StopWatch timer;
    int findPointLoopLimiter = 5;
    float differPos=5;

    private void Awake()
    {
        timer = StopWatch.Summon(Random.Range(5.0f, 12.0f), EnemySpawn, gameObject);
        timer.IsActive = false;
    }

    private void Start()
    {
        playerTransform = PlayerManager.GetManager.GetPlObj.transform;
        timer.IsActive = true;
    }

    void EnemySpawn()
    {
        var pos = SpawnPointFinder(0);
        if (pos == null) return;

        var enemy = EnemySpawnManager.GetEnemy();
        if (enemy == null) return;

        enemy.transform.position = (Vector3)pos;
        enemy.life.AddLastword(() => EnemySpawnManager.SetEnemy(enemy));
        enemy.gameObject.SetActive(true);
    }

    Vector3 SpawnPointSelector()
    {
        Vector3 desirePos = playerTransform.position;
        float angle = Random.Range(0f, 2 * Mathf.PI);
        desirePos.x += Mathf.Cos(angle);
        desirePos.z += Mathf.Sin(angle);

        return desirePos;
    }

    Vector3? SpawnPointFinder(int loopCount)
    {
        Vector3 desirePos = SpawnPointSelector();

        var terrain = NewMapTerrainData.GetTerrain;
        if (!IsOutHeight(terrain, desirePos)) return desirePos;
        else desirePos.x += differPos;
        if (IsOutHeight(terrain, desirePos)) desirePos.x -= 2 * differPos;
        else return desirePos;
        if (IsOutHeight(terrain, desirePos)) desirePos.z += differPos;
        else return desirePos;
        if (IsOutHeight(terrain, desirePos)) desirePos.z -= 2 * differPos;
        else return desirePos;

        if (loopCount >= findPointLoopLimiter) return null;
        else SpawnPointFinder(++loopCount);

        return null;
    }

    bool IsOutHeight(Terrain terrain,Vector3 desirePos)
    {
        return terrain.terrainData.GetInterpolatedHeight(desirePos.x / terrain.terrainData.size.x, desirePos.z / terrain.terrainData.size.z) >= 15;
    }
}