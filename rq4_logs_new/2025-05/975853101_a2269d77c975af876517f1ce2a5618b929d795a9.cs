using System.Collections;
using System.Collections.Generic;
using UnityEditor;
using UnityEngine;

public class Spawner : MonoBehaviour
{
    public string prefabName = "TakaObject";
    public GameObject prefab;
    public Material[]materials;
    public float spawnInterval = 2f;

    void Start()
    {
        prefab = Resources.Load<GameObject>($"Prefabs/{prefabName}");
        if(prefab == null)
        {
            Debug.LogError($"Prefab'{prefabName}'がResources/Prefabsに見つかりません!");
        }

        materials = Resources.LoadAll<Material>("Materials");
        if(materials == null || materials.Length == 0)
        {
            Debug.LogWarning("Materials が読み込めませんでした。Resource/Materialsフォルダを確認してください。");
        }
        InvokeRepeating(nameof(SpawnObject),0f,spawnInterval);        
    }

    void SpawnObject()
    {
        if(prefab == null)
        {
            Debug.LogWarning("Prefabが設定されていません！");
            return;
        }
        Vector3 pos = new Vector3(Random.Range(-5f,5f),1f,Random.Range(-5f,5f));
        GameObject obj = Instantiate(prefab,pos,Quaternion.identity);

        int index = Random.Range(0,materials.Length);
        obj.GetComponent<Renderer>().material = materials[index];
    }
}