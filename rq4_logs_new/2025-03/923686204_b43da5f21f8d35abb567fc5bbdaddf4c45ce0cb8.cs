using UnityEngine;
using TMPro;
using System.Collections;
using ObjectPoolings;

public class DamageTextManager : MonoBehaviour
{
    public static DamageTextManager Instance { get; private set; }

    [Header("Object Pooling")]
    [SerializeField] private GameObject damageTextPrefab;

    [SerializeField] private float displayDuration = 2f;

    private void Awake()
    {
        if (Instance != null && Instance != this)
        {
            Destroy(gameObject);
            return;
        }
        Instance = this;
    }

    public void ShowDamageText(Vector3 position, float damage)
    {
        var (damageTextObj, pool) = ObjectPooling.GetOrCreate(damageTextPrefab, position, damageTextPrefab.transform.rotation);

        var tmp = damageTextObj.GetComponent<TextMeshPro>();
        if (tmp != null)
        {
            tmp.text = damage.ToString();
        }
        else
        {
            Debug.LogWarning("Pooled DamageText object is missing a TextMeshPro component!");
        }

        damageTextObj.SetActive(true);

        StartCoroutine(ReleaseAfterDelay(damageTextObj, pool, displayDuration));
    }

    private IEnumerator ReleaseAfterDelay(GameObject damageTextObj, PrefabPool pool, float delay)
    {
        yield return new WaitForSeconds(delay);

        pool.Release(damageTextObj);
    }
}