using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ViewerSpawner : MonoBehaviour
{
    // インスタンス化するゲームオブジェクト
    public GameObject entityToSpawn;

    // 上で定義した ScriptableObject のインスタンス
    public ViewerScriptableObject spawnManagerValues;

    // 作成されたエンティティの名に追加され、それぞれが作成されるたびにインクリメントされます。
    int instanceNumber = 1;

    void Start()
    {
        SpawnEntities();
    }

    void SpawnEntities()
    {
        int currentSpawnPointIndex = 0;
        var spawnManagerValues = Resources.Load($"SampleViewerDataNew") as ViewerScriptableObject;

        for (int i = 0; i < spawnManagerValues.numberOfPrefabsToCreate; i++)
        {
            // 現時点のスポーン位置でプレハブのインスタンスを作成します。
            GameObject currentEntity = Instantiate(entityToSpawn, spawnManagerValues.spawnPoints[currentSpawnPointIndex], Quaternion.identity);

            // インスタンス化したエンティティの名前が ScriptableObject で定義した文字列になるように設定し、次に、固有の番号を追加します。
            currentEntity.name = spawnManagerValues.prefabName + instanceNumber;

            // スポーン位置の次のインデックスに移動します。インデックスを超えた場合は、最初に戻ります。
            currentSpawnPointIndex = (currentSpawnPointIndex + 1) % spawnManagerValues.spawnPoints.Length;

            instanceNumber++;
            Debug.Log($"instanceNumber: {instanceNumber}");
        }
    }
}