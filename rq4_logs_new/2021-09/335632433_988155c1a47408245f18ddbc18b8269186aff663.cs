using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Burn_Firstset : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }

    private void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.tag == "Floor")
        {
            Destroy(GetComponent<SphereCollider>());
            gameObject.GetComponent<Rigidbody>().useGravity = false;
            gameObject.GetComponent<EnemyPunch>().enabled = true;
            gameObject.GetComponent<MeshCollider>().enabled = true;
            Destroy(this);

        }
    }
}