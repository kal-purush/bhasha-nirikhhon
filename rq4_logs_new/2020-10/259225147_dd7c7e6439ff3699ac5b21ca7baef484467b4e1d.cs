using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Id47_SmokeBomb : MonoBehaviour
{
    [SerializeField] GameObject smokeBombParticle;
    GameObject smokeBombObj;

    // Start is called before the first frame update
    void Start()
    {
        transform.parent = null;
        smokeBombObj = Instantiate(smokeBombParticle, transform);
    }

    // Update is called once per frame
    void Update()
    {
        if (gameObject.transform.childCount == 0) Destroy(gameObject);
    }
}