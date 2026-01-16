using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Fixing : MonoBehaviour
{
    public Animator anim;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }


    private void OnTriggerEnter(Collider other)
    {
        if (this.tag == "Gary" && other.tag == "Player" && Input.GetMouseButton(0))
        {
            anim.SetTrigger("GaryHappy");
        }

        if (this.tag == "Ricky" && other.tag == "Player" && Input.GetMouseButton(0))
        {
            anim.SetTrigger("RickHappy");
        }

        if (this.tag == "Rachel" && other.tag == "Player" && Input.GetMouseButton(0))
        {
            anim.SetTrigger("RachHappy");
        }
    }
}