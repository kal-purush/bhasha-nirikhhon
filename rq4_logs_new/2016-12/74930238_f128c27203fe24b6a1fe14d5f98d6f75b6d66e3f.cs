using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using UnityEngine.SceneManagement;


public class cortinilla : MonoBehaviour {

	/*Programado por José
	 Este es el código para la cortinilla general.
	 La cortinilla es un efecto para hacer más amena la transición entre escenas. Este codigo en especifico ofrece un efecto
	 muy sencillo, obscurece y aclara la pantalla para marcar las entradas y salidas.
	 Esto lo hace afectando el alfa de una imagen de color negro a cierta velocidad dada.
	 Al terminar manda a la escena que se le indique.
	*/


	public string siguienteEscena="";
	public float velocidadEntrada=5.0f;
	public float velocidadSalida=5.0f;


	public string estado="Entrada";


	Image cuerpoImagen;
	public float alfa=1.0f;

	// Use this for initialization
	void Start () {
		cuerpoImagen=this.GetComponent<Image>();
		estado="Entrada";
	}



	// Update is called once per frame
	void Update () {
	
		if(estado=="Entrada"){
			Entrar();
		}else if(estado=="Salida"){
			Salir();
		}

	}


	void Entrar(){
		alfa-=velocidadEntrada*Time.deltaTime;
		cuerpoImagen.color=new Vector4(0.0f,0.0f,0.0f,alfa);
		if(alfa<0.0f){
			estado="Salida";
			this.gameObject.SetActive(false);
		}
	}


	void Salir(){
		alfa+=velocidadSalida*Time.deltaTime;
		cuerpoImagen.color=new Vector4(0.0f,0.0f,0.0f,alfa);
		if(alfa>1.3f){
			estado="Terminado";
			SceneManager.LoadScene(siguienteEscena);
		}
	}
		
}