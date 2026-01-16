using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Objectpooling : MonoBehaviour
{
    public static Objectpooling poolingManager;

    [SerializeField] List<GameObject> AsteroidList;
    [SerializeField] GameObject asteroidPrefab;
    int asteroidPoolSize = 10;
    [SerializeField] float randomPosY;
    [SerializeField] float randomScale;

    [SerializeField] List<GameObject> effectList;
    [SerializeField] GameObject effectPrefab;
    int effectPoolSize = 5;

    void Awake()
    {
        if (poolingManager == null) poolingManager = this;
        else if (poolingManager != this) Destroy(gameObject);

        StartCoroutine(CreateAsteroidPool());
        StartCoroutine(CreateEffectPool());
    }

    IEnumerator CreateAsteroidPool()
    {
        yield return new WaitForSeconds(1f);

        GameObject AsteroidGroup = new GameObject("AsteroidGroup");
        for (int i = 0; i < asteroidPoolSize; i++)
        {
            var asteroid = Instantiate(asteroidPrefab, AsteroidGroup.transform);
            asteroid.SetActive(false);
            AsteroidList.Add(asteroid);
        }

        StartCoroutine(CreateAsteroid());
    }

    IEnumerator CreateAsteroid()
    {
        while (true)
        {
            foreach (var asteroid in AsteroidList)
            {
                randomPosY = Random.Range(-4f, 4f);
                randomScale = Random.Range(1f, 3f);
                asteroid.transform.position = new Vector3(10f, randomPosY, 0f);
                asteroid.transform.localScale = Vector3.one * randomScale;
                asteroid.SetActive(true);
                break;
            }
            yield return new WaitForSeconds(3f);
        }
    }

    IEnumerator CreateEffectPool()
    {
        yield return new WaitForSeconds(1f);

        GameObject EffectGroup = new GameObject("EffectGroup");
        for (int i = 0; i < effectPoolSize; i++)
        {
            var eff = Instantiate(effectPrefab, EffectGroup.transform);
            eff.SetActive(false);
            effectList.Add(eff);
        }
    }

    public GameObject GetEffectPool()
    {
        for (int i = 0; i < effectPoolSize; i++)
            if (effectList[i].activeSelf == false)
                return effectList[i];

        return null;
    }
}