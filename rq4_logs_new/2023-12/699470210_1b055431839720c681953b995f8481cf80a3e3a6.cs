using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RampScript : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void OnCollisionEnter2D(Collision2D collision) {
        if(collision.gameObject.TryGetComponent<PlayerController>(out PlayerController player)){
            player.EnterRampMode();
        }
    }

    private void OnCollisionExit2D(Collision2D collision) {
        if(collision.gameObject.TryGetComponent<PlayerController>(out PlayerController player)) {
            player.ExitRampMode();
        }
    }
}