using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NewBehaviourScript : MonoBehaviour
{
    [SerializeField] GameObject obj;
    static readonly int max= 10;

    // Start is called before the first frame update
    void Start()
    {
        //InvokeRepeating("Instant", 1f, 0.5f);

        Invoke("Instant", 3);

        if (obj.GetComponent<Rigidbody>() == null)
        {
            obj.AddComponent<Rigidbody>();
        }
    }

    void Instant()
    {
        for (int i = 0; i < 10; i++)
        {
            var o = Instantiate(obj, transform);
            var rb = o.GetComponent<Rigidbody>();

            var x = Random.Range(-max, max);
            var y = Random.Range(-max, max);
            var z = Random.Range(-max, max);

            rb.AddRelativeFor​​ce(Random.onUnitSphere * max, ForceMode.VelocityChange);
        }
    }
}