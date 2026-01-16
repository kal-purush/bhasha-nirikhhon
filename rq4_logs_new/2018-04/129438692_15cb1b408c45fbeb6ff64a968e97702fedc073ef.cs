using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraFollow : MonoBehaviour {

    //crea una variable de tipus Player Movement
    public Player_Movement myPlayer;
   
    //ultima posicio del pers, un vector (x, y, z)
    private Vector3 lastPlayerPos;
    
    //distancia que s'ha de moure la camera
    private float distMove;


	// Use this for initialization
	void Start () {
        
        //la nostra variable = a l'objecte que trobi de tipus Player Movement
        myPlayer = FindObjectOfType<Player_Movement>();
        
        //inicialitzem la variable a la ultima posicio del player
        lastPlayerPos = myPlayer.transform.position;
    }
	
	// Update is called once per frame
	void Update () {
        
        //distancia que s'ha de moure la camera : posicio actual - ultima posicio
        distMove = myPlayer.transform.position.x - lastPlayerPos.x;
        
        //la distancia que s'ha de moure afegida en eixos de x del vector de la posicio de la camera
        transform.position = new Vector3 (transform.position.x + distMove, transform.position.y, transform.position.z);
        
        //actualitzem la nova last position del player
        lastPlayerPos = myPlayer.transform.position;
	}
}