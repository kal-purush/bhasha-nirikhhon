using UnityEngine;
using System.Collections;

public class movimientoSenoidal : MonoBehaviour {

	/*
	Programado por José
	Este codigo sirve para hacer un movimiento senoidal sobre el eje Y.
	El cuerpo ira de arriba hacia abajo constantemente por tiempo indeterminado.
	Utilizamos la funsion Seno para hacer el truco, aplicamos un cambio a la posicion Y del RectTransform que es 
	multiplicado por un valor que depende de la funsión Seno utilizando un ángulo que va variando con respecto al tiempo.  
	*/


	public GameObject objeto;

	RectTransform cuerpo;



	public float velocidad=5.0f;
	public float amplitud=1.0f;


	float angulo=180.0f;
	float valor=0.0f;

	// Use this for initialization
	void Start () {
		cuerpo=objeto.GetComponent<RectTransform>();
	}


	// Update is called once per frame
	void Update () {
		movimientoVertical();
	}



	//Manda a llamar la funsion que altera el angulo
	//Hace el calculo de seno con el nuevo angulo obtenido y se lo añade a una variable valor
	//Aplica esa variable valor a la posicion en Y del objeto. 
	void movimientoVertical(){
		cambioDeAngulo();
		valor=amplitud*Mathf.Sin(angulo);
		cuerpo.localPosition+=new Vector3(0.0f,valor,0.0f);
	}



	//Varia un angulo con respecto al tiempo. Al pasar 360 grados los regresa a cero.
	void cambioDeAngulo(){
		angulo+=velocidad*Time.deltaTime;
		if(angulo>=360)
			angulo=0;
	}

}