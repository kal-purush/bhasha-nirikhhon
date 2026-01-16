    // [SerializeField] private float stalkMstSpeed = 1f;
    // [SerializeField] private float stalkAccRatio = 0.8f;
    private bool isFrozen; // 表示怪物是否处于冰冻状态
    public static ParticleEffectManager Instance;//单例
    public GameObject particlePrefab; // 默认特效预制体
    
                                    Color endColor, float duration = -1f)

        var particleSystem = particleEffect.GetComponent<ParticleSystem>();
        var particleMain = particleSystem.main;
        particleMain.startColor = startColor;


        StartCoroutine(FadeInAndOut(particleSystem, particleEffect, duration, startColor, endColor));
    private IEnumerator FadeInAndOut(ParticleSystem particleSystem, GameObject particleEffect, float duration, Color startColor, Color endColor)
        float elapsedTime = 0f;
        while (elapsedTime < duration)
            float lerpValue = elapsedTime / duration;
            Color lerpedColor = Color.Lerp(startColor, endColor, lerpValue);

            // 修改粒子系统的颜色
            var main = particleSystem.main;
            main.startColor = lerpedColor;

            elapsedTime += Time.deltaTime;
        // 确保结束时颜色是endColor
        var finalMain = particleSystem.main;
        finalMain.startColor = endColor;

        // 在淡出后销毁特效
            Destroy(particleEffect);
﻿// using System.Collections.Generic;
// using UnityEngine;
//
// public class ObjectPooler : MonoBehaviour
// {
//     [System.Serializable]
//     public class Pool
//     {
//         public string tag;
//         public GameObject prefab;
//         public int size;
//     }
//
//     public List<Pool> pools;
//     public Dictionary<string, Queue<GameObject>> poolDictionary;
//
//     #region Singleton
//     public static ObjectPooler Instance;
//
//     private void Awake()
//     {
//         Instance = this;
//     }
//     #endregion
//
//     private void Start()
//     {
//         poolDictionary = new Dictionary<string, Queue<GameObject>>();
//
//         foreach (Pool pool in pools)
//         {
//             Queue<GameObject> objectPool = new Queue<GameObject>();
//
//             for (int i = 0; i < pool.size; i++)
//             {
//                 GameObject obj = Instantiate(pool.prefab);
//                 obj.SetActive(false);
//                 objectPool.Enqueue(obj);
//             }
//
//             poolDictionary.Add(pool.tag, objectPool);
//         }
//     }
//
//     public GameObject SpawnFromPool(string tag, Vector3 position, Quaternion rotation)
//     {
//         if (!poolDictionary.ContainsKey(tag))
//         {
//             Debug.LogWarning("Pool with tag " + tag + " doesn't exist.");
//             return null;
//         }
//
//         GameObject objectToSpawn = poolDictionary[tag].Dequeue();
//
//         if (!objectToSpawn.activeSelf)
//         {
//             objectToSpawn.SetActive(true);
//             objectToSpawn.transform.position = position;
//             objectToSpawn.transform.rotation = rotation;
//
//             poolDictionary[tag].Enqueue(objectToSpawn);
//         }
//
//         return objectToSpawn;
//     }
//
//     public void ReturnToPool(string tag, GameObject objectToReturn)
//     {
//         if (!poolDictionary.ContainsKey(tag))
//         {
//             Debug.LogWarning("Pool with tag " + tag + " doesn't exist.");
//             return;
//         }
//
//         objectToReturn.SetActive(false);
//         poolDictionary[tag].Enqueue(objectToReturn);
//     }
//
// }