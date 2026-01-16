using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnnemySpawnScript : MonoBehaviour
{
    [SerializeField] float spawningDistance;
    GameObject player;
    public GameObject pigPrefab;
    GameObject pig;
    bool isActive;

    int countPig;


    void Start()
    {
        countPig = 0;
    }


    // Update is called once per frame
    void Update()
    {
        player = GameObject.FindGameObjectWithTag("Player");
        if (Vector3.Distance(player.transform.position, transform.position) < spawningDistance)
        {
            if (!isActive)
            {
                pig = Instantiate(pigPrefab, transform.position, Quaternion.identity);
                countPig++;
                isActive = true;
                if (countPig < 5)
                {
                    Invoke("Desactivate", 5);
                }
            }
        }
    }

    void Desactivate()
    {
        isActive = false;
    }
}
