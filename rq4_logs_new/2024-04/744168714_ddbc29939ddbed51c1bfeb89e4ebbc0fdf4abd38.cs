using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Blooddestroy : MonoBehaviour
{
    // Start is called before the first frame update
    public GameObject BloodDecal;

    // Update is called once per frame
    void Awake()
    {
        StartCoroutine(destroy());
    }

    IEnumerator destroy()
    {
        yield return new WaitForSeconds(2f);
        //Instantiate(BloodDecal, transform.position, Quaternion.identity);
        Destroy(gameObject);

    }
}