using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FragileFloor : MonoBehaviour
{
    // Start is called before the first frame update
    [SerializeField] float initialStrength = 1;
    [SerializeField] private Texture defaultTexture;
    [SerializeField] private Texture crackedTexture;
    float strength;
    private bool collisionWithPlayer = false;


    void Start()
    {   strength = 1;
        Renderer renderer = gameObject.GetComponent<Renderer>();
        renderer.material.mainTexture = defaultTexture;
    }

    private void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.name == "Player"){
            collisionWithPlayer=true;
        }
    }
    private void OnCollisionExit(Collision collision)
    {
        if (collision.gameObject.name == "Player"){
            collisionWithPlayer=false;
        }
    }

    void Update()
    {
        if (collisionWithPlayer == true){
            strength = strength - Time.deltaTime;
        }
        if (strength < initialStrength*0.5){crack();}
        if (strength < 0){destroyYourself();}


    }

    private void destroyYourself(){        
        Destroy(gameObject);
    }

    private void crack(){
        Renderer renderer = gameObject.GetComponent<Renderer>();
        renderer.material.mainTexture = crackedTexture;
    }
 
}