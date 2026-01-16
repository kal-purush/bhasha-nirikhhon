using UnityEngine;
using System.Collections;
using System.IO;

public class EnemyLikeStatus : MonoBehaviour {

    [SerializeField]
    int enemyHP;
    float  attackPower;
    int attribute;


    EnemyParameter enemyParameter;

	void Start ()
    {
        enemyParameter = GetComponent<EnemyParameter>();
        Debug.Log(enemyParameter.GetAttribute(2));
    }
	
	void Update ()
    {
        
	}


    void EnemyLikeStatusManeger()
    {
        attribute = Random.Range(0,3);
        enemyParameter.GetAttribute(attribute);

    }
}