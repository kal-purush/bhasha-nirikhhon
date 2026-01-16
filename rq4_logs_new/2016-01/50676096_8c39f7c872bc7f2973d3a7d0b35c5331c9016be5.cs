using UnityEngine;
using System.Collections;

public class Fireball : MonoBehaviour {
    GameObject objec;
    void Start()
    {
        objec = transform.parent.gameObject;
    }
    void OnTriggerEnter(Collider other)
    {

        if (other.gameObject.tag == "Player")
        {
            Debug.Log("touched fireboll!");
            Destroy(other.gameObject);
        }
        if (other.gameObject.tag == "Fireball")
        {
            return;
        }
        else
        {
            Debug.Log("objekt!");
            Destroy(objec);
        }
    }
}