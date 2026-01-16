using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScoreCount : MonoBehaviour
{
    public GameObject boss;
    [SerializeField]
    GameObject mochi;
    [SerializeField]
    float mochiHeight;
    [SerializeField]
    int enemyCount;

    [SerializeField]
    int point;
    // Start is called before the first frame update
    void Start()
    {
        point = 0;
    }

    // Update is called once per frame
    void Update()
    {
        if (mochi == null)
        {
            enemyCount = mochi.transform.childCount;
        }
        else
        {
            enemyCount = this.gameObject.transform.childCount;
        }
    }

    void OnTriggerEnter(Collider other)
    {
        //どうしようか考えてる
        if (other.gameObject == boss)
        {
            mochiHeight = this.gameObject.GetComponent<CapsuleCollider>().height;
            //式は仮置きぃ
            point += (int)mochiHeight + enemyCount;
            Debug.Log(point);
        }
    }
}