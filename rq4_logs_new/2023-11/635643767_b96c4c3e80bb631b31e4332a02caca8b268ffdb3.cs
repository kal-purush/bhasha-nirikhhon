using UnityEngine;

public class PrefabPoisonPool : MonoBehaviour
{
    [SerializeField] private float triggerInterval = default;
    [SerializeField] private float damageMin = default;
    [SerializeField] private float damageMax = default;

    private float finalDamage = default;
    private float triggerTimer = default;

    //===========================================================================
    private void OnEnable()
    {
        EventManager.BeforeSceneUnloadEvent += EventManager_BeforeSceneUnloadEvent;
        triggerTimer = triggerInterval;
    }

    private void Update()
    {
        triggerTimer -= Time.deltaTime;
        if (triggerTimer <= 0.0f)
        {
            triggerTimer = triggerInterval;
            DealDamage();
        }
    }

    private void OnDisable()
    {
        EventManager.BeforeSceneUnloadEvent -= EventManager_BeforeSceneUnloadEvent;
    }

    //===========================================================================
    private void EventManager_BeforeSceneUnloadEvent()
    {
        gameObject.SetActive(false);
        Destroy(gameObject);
    }

    //===========================================================================
    private void DealDamage()
    {
        finalDamage = Random.Range(damageMin, damageMax);

        Collider2D[] collider2DArray = Physics2D.OverlapCircleAll(transform.position, 2.0f);
        foreach (Collider2D collider2D in collider2DArray)
        {
            if (collider2D.CompareTag("Enemy"))
                collider2D.GetComponent<EnemyHealth>().UpdateCurrentHealth(-finalDamage);

            if (collider2D.CompareTag("FinalBoss"))
                collider2D.transform.parent.GetComponent<EnemyHealth>().UpdateCurrentHealth(-finalDamage);
        }
    }
}