using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class healtpack : MonoBehaviour
{
    float cura;
    // Start is called before the first frame update
    void Start()
    {
        cura = .5f;
    }

    // Update is called once per frame
    private void OnTriggerStay(Collider other) {
        if(other.name == "aj"){
            other.GetComponent<vida_aj>().TakeHeal(cura);
           // other.GetComponent<AjControl>().dolor();
         
        }
        
    }
}