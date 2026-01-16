using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ButtonTrigger : MonoBehaviour
{
    public bool isPressed = false;
    Animator animator;
    // Start is called before the first frame update
    void Start()
    {
        animator = this.GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

       private void OnTriggerEnter2D(Collider2D other) {

        Debug.Log(other.gameObject.name);
        if(other.gameObject.tag == "Player" || other.gameObject.tag == "Bomb"){
            isPressed = true;
            animator.SetBool("IsPressed",isPressed);
        }    
    }

    private void OnTriggerExit2D(Collider2D other) {
         Debug.Log(other.gameObject.name);
            if(other.gameObject.tag == "Player" || other.gameObject.tag == "Bomb"){
            isPressed = false;
            animator.SetBool("IsPressed",isPressed);
        }
    }
}