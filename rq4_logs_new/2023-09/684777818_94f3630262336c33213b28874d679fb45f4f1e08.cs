using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TrapController : MonoBehaviour
{
    private GameObject trap;
    private Rigidbody rb;
    // Start is called before the first frame update
    void Start()
    {
        trap = GameObject.Find("Trap");
        rb = GetComponent<Rigidbody>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void OnCollisionEnter(Collision collision)
    {
        rb.useGravity = true;

        StartCoroutine(hide());
    }

    private IEnumerator hide()
    {
        yield return new WaitForSeconds(2);

        trap.SetActive(false);
    }
}