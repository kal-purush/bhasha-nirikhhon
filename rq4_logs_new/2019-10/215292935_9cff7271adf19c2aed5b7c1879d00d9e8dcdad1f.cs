using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GenerateMonstersRandomly : MonoBehaviour
{
    public GameObject theMonster;
    private float xPos;
    private float zPos;
    public float maxNumberOfMonsterToSpawned;
    private int monstersCount = 0;
    public float delayToSpawnAnotherMonster; //in seconds
    // Start is called before the first frame update
    void Start()
    {
        StartCoroutine(GenerateRandomlyMonsters());
    }

    IEnumerator GenerateRandomlyMonsters() {
        while (monstersCount < maxNumberOfMonsterToSpawned) {
            xPos = Random.Range(gameObject.transform.position.x - gameObject.transform.localScale.x / 2, 
                                gameObject.transform.position.x + gameObject.transform.localScale.x / 2);
            zPos = Random.Range(gameObject.transform.position.z - gameObject.transform.localScale.z / 2, 
                                gameObject.transform.position.z + gameObject.transform.localScale.z / 2);
            Instantiate(theMonster, new Vector3(xPos, 0, zPos), Quaternion.identity);
            yield return new WaitForSeconds(delayToSpawnAnotherMonster);
            monstersCount ++;
        }
    }

}