using UnityEngine;
using System.Collections;

public class HealingScript : MonoBehaviour {
	
	public caracteristicaDrone datos;
	public float temporizador;
	public int cantidad;
	float cuenta;
	GameController jugador;
	
	void Start(){
		jugador= GameObject.FindGameObjectWithTag("GameController").GetComponent<GameController>();
		cuenta= temporizador;
	}
	
	void goToBox(){
		if (datos.box != null)
			transform.position = Vector2.MoveTowards (this.transform.position, datos.box.transform.position, 300*Time.deltaTime);
	}
	
	public void SetBox(GameObject box){
		if (datos.box != null) {
			if(this.gameObject == datos.box.GetComponent<BoxScript>().dron.gameObject){
				datos.box.GetComponent<BoxScript>().dron = null;
				datos.box.GetComponent<BoxScript> ().taken = false;
			}
		}
		datos.box = box;
		box.GetComponent<BoxScript>().taken = true;
		box.GetComponent<BoxScript> ().SetDrone (this.gameObject);
		Debug.Log ("caja " + box.GetComponent<BoxScript> ().id);
	}
	
	public GameObject GetBox(){
		return(datos.box);
	}
	
	// Update is called once per frame
	void Update () 
	{	
		if(!datos.pausado)
		{
			goToBox();
			//añade energia al jugador
			cuenta-= Time.deltaTime;
			if(cuenta <= 0f)
			{
				jugador.vida+= cantidad;
				cuenta= temporizador;
			}
		}
	}
	
	void OnDestroy(){
		GameObject.Find ("Pick&DropController").SendMessage ("CheckDestroyed", this.gameObject);
		datos.box.GetComponent<BoxScript>().taken= false;
		datos.box.GetComponent<BoxScript> ().dron = null;
	}
}