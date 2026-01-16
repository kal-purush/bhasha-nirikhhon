using UnityEngine;

public class GenerateEnemies : MonoBehaviour
{
    [SerializeField] GameObject enemyPrefab;
    [SerializeField] RuntimeAnimatorController animatorCon;

    [SerializeField] float timer;
    float timeCounter;

    private void Start()
    {
        timeCounter = 0;
    }
    private void Update()
    {
        if (timeCounter < 0)
        {
            GameObject go = Instantiate(enemyPrefab);
            go.GetComponent<Animator>().runtimeAnimatorController = animatorCon;
            timeCounter = timer;
            return;
        }
        timeCounter -= Time.deltaTime;
    }
}