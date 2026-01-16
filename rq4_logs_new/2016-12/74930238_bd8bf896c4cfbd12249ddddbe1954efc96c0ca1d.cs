using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class carrusel : MonoBehaviour {

	/*
	Este código sirve para el carrusel del menú principal.
	Captura las teclas y ajusta el menú al estado que le corresponde.
	También ejecuta la acción que fue seleccionada.
	*/

	public AudioSource SFX_navegacion;

	public GameObject Nuevo_Juego;
	public GameObject Continuar;
	public GameObject Opciones;
	public GameObject Creditos;
	public GameObject Salir;


	public GameObject Texto_Nuevo_Juego;
	public GameObject Texto_Continuar;
	public GameObject Texto_Opciones;
	public GameObject Texto_Creditos;
	public GameObject Texto_Salir;




	public GameObject brillo_NuevoJuego;
	public GameObject brillo_Continuar;
	public GameObject brillo_Opciones;
	public GameObject brillo_Creditos;
	public GameObject brillo_Salir;


	public int estado=1;

	Vector3 primerTamanio=new Vector3(1.0f,1.0f,1.0f);
	Vector3 segundoTamanio=new Vector3(0.8f,0.8f,1.0f);
	Vector3 tercerTamanio=new Vector3(0.7f,0.7f,1.0f);

	Color alfaCentral=new Color(212.0f/255.0f,242.0f/255.0f,230.0f/255.0f,1.0f);
	Color alfaSecundario=new Color(212.0f/255.0f,242.0f/255.0f,230.0f/255.0f,0.6f);
	Color alfaTerciario=new Color(212.0f/255.0f,242.0f/255.0f,230.0f/255.0f,0.4f);

	Color alfaCentralB=new Color(0.0f,0.0f,0.0f,222.0f/255.0f);
	Color alfaSecundarioB=new Color(0.0f,0.0f,0.0f,0.7f);
	Color alfaTerciarioB=new Color(0.0f,0.0f,0.0f,0.5f);


	Vector3 posicionCentral=new Vector3 (0.0f,0.0f,0.0f); 
	Vector3 posicionSecundaria=new Vector3 (-20.0f,80.0f,0.0f);
	Vector3 posicionTerciaria=new Vector3 (-40.0f,160.0f,0.0f);

	Vector3 posicionSecundariaBaja=new Vector3 (-20.0f,-80.0f,0.0f);
	Vector3 posicionTerciariaBaja=new Vector3 (-40.0f,-160.0f,0.0f);



	// Use this for initialization
	void Start () {
		seleccionarEstado();
	}
	
	// Update is called once per frame
	void Update () {
	
		if(Input.GetKeyDown(KeyCode.W)||Input.GetKeyDown(KeyCode.A)||Input.GetKeyDown(KeyCode.LeftArrow)||Input.GetKeyDown(KeyCode.UpArrow)){
			SFX_navegacion.Play();
			estado--;
			if (estado<=0){
				estado=5;
			}
			seleccionarEstado();
		}


		if(Input.GetKeyDown(KeyCode.S)||Input.GetKeyDown(KeyCode.DownArrow)||Input.GetKeyDown(KeyCode.RightArrow)||Input.GetKeyDown(KeyCode.D)){
			SFX_navegacion.Play();
			estado++;
			if (estado>=6){
				estado=1;
			}
			seleccionarEstado();
		}
	}


	void seleccionarEstado(){
		print("Nuevo Estado");

		switch(estado){

			case 1:
				estado_NuevoJuego();
				break;

			case 2:
				estado_Continuar();
				break;

			case 3:
				estado_Opciones();
				break;


			case 4:
				estado_Salir();
				break;


			case 5:
				estado_Creditos();
				break;	

			default:
				print("Algo salio terriblemente mal. Estado = "+estado);
				break;
		}

	}



	void estado_NuevoJuego(){


		Salir.GetComponent<RectTransform>().localScale=tercerTamanio;
		Creditos.GetComponent<RectTransform>().localScale=segundoTamanio;
		Nuevo_Juego.GetComponent<RectTransform>().localScale=primerTamanio;
		Continuar.GetComponent<RectTransform>().localScale=segundoTamanio;
		Opciones.GetComponent<RectTransform>().localScale=tercerTamanio;





		Salir.GetComponent<RectTransform>().localPosition=posicionTerciaria;
		Creditos.GetComponent<RectTransform>().localPosition=posicionSecundaria;
		Nuevo_Juego.GetComponent<RectTransform>().localPosition=posicionCentral;
		Continuar.GetComponent<RectTransform>().localPosition=posicionSecundariaBaja;
		Opciones.GetComponent<RectTransform>().localPosition=posicionTerciariaBaja;


		Texto_Salir.GetComponent<Text>().color=alfaTerciario;
		Texto_Creditos.GetComponent<Text>().color=alfaSecundario;
		Texto_Nuevo_Juego.GetComponent<Text>().color=alfaCentral;
		Texto_Continuar.GetComponent<Text>().color=alfaSecundario;
		Texto_Opciones.GetComponent<Text>().color=alfaTerciario;



		Salir.GetComponent<Image>().color=alfaTerciarioB;
		Creditos.GetComponent<Image>().color=alfaSecundarioB;
		Nuevo_Juego.GetComponent<Image>().color=alfaCentralB;
		Continuar.GetComponent<Image>().color=alfaSecundarioB;
		Opciones.GetComponent<Image>().color=alfaTerciarioB;





		brillo_NuevoJuego.SetActive(true);
		brillo_Continuar.SetActive(false);
		brillo_Creditos.SetActive(false);



		print("Estado 1");

	}

	void estado_Continuar(){

		Creditos.GetComponent<RectTransform>().localScale=tercerTamanio;
		Nuevo_Juego.GetComponent<RectTransform>().localScale=segundoTamanio;
		Continuar.GetComponent<RectTransform>().localScale=primerTamanio;
		Opciones.GetComponent<RectTransform>().localScale=segundoTamanio;
		Salir.GetComponent<RectTransform>().localScale=tercerTamanio;



		Creditos.GetComponent<RectTransform>().localPosition=posicionTerciaria;
		Nuevo_Juego.GetComponent<RectTransform>().localPosition=posicionSecundaria;
		Continuar.GetComponent<RectTransform>().localPosition=posicionCentral;
		Opciones.GetComponent<RectTransform>().localPosition=posicionSecundariaBaja;
		Salir.GetComponent<RectTransform>().localPosition=posicionTerciariaBaja;





		Texto_Creditos.GetComponent<Text>().color=alfaTerciario;
		Texto_Nuevo_Juego.GetComponent<Text>().color=alfaSecundario;
		Texto_Continuar.GetComponent<Text>().color=alfaCentral;
		Texto_Opciones.GetComponent<Text>().color=alfaSecundario;
		Texto_Salir.GetComponent<Text>().color=alfaTerciario;



		Creditos.GetComponent<Image>().color=alfaTerciarioB;
		Nuevo_Juego.GetComponent<Image>().color=alfaSecundarioB;
		Continuar.GetComponent<Image>().color=alfaCentralB;
		Opciones.GetComponent<Image>().color=alfaSecundarioB;
		Salir.GetComponent<Image>().color=alfaTerciarioB;




		brillo_NuevoJuego.SetActive(false);
		brillo_Continuar.SetActive(true);
		brillo_Opciones.SetActive(false);




		print("Estado 2");
	}

	void estado_Opciones(){

		Nuevo_Juego.GetComponent<RectTransform>().localScale=tercerTamanio;
		Continuar.GetComponent<RectTransform>().localScale=segundoTamanio;
		Opciones.GetComponent<RectTransform>().localScale=primerTamanio;
		Salir.GetComponent<RectTransform>().localScale=segundoTamanio;
		Creditos.GetComponent<RectTransform>().localScale=tercerTamanio;



		Nuevo_Juego.GetComponent<RectTransform>().localPosition=posicionTerciaria;
		Continuar.GetComponent<RectTransform>().localPosition=posicionSecundaria;
		Opciones.GetComponent<RectTransform>().localPosition=posicionCentral;
		Salir.GetComponent<RectTransform>().localPosition=posicionSecundariaBaja;
		Creditos.GetComponent<RectTransform>().localPosition=posicionTerciariaBaja;




		Texto_Nuevo_Juego.GetComponent<Text>().color=alfaTerciario;
		Texto_Continuar.GetComponent<Text>().color=alfaSecundario;
		Texto_Opciones.GetComponent<Text>().color=alfaCentral;
		Texto_Salir.GetComponent<Text>().color=alfaSecundario;
		Texto_Creditos.GetComponent<Text>().color=alfaTerciario;


		Nuevo_Juego.GetComponent<Image>().color=alfaTerciarioB;
		Continuar.GetComponent<Image>().color=alfaSecundarioB;
		Opciones.GetComponent<Image>().color=alfaCentralB;
		Salir.GetComponent<Image>().color=alfaSecundarioB;
		Creditos.GetComponent<Image>().color=alfaTerciarioB;




		brillo_Continuar.SetActive(false);
		brillo_Opciones.SetActive(true);
		brillo_Salir.SetActive(false);




		print("Estado 3");
	}



	void estado_Salir(){
		Continuar.GetComponent<RectTransform>().localScale=tercerTamanio;
		Opciones.GetComponent<RectTransform>().localScale=segundoTamanio;
		Salir.GetComponent<RectTransform>().localScale=primerTamanio;
		Creditos.GetComponent<RectTransform>().localScale=segundoTamanio;
		Nuevo_Juego.GetComponent<RectTransform>().localScale=tercerTamanio;


		Continuar.GetComponent<RectTransform>().localPosition=posicionTerciaria;
		Opciones.GetComponent<RectTransform>().localPosition=posicionSecundaria;
		Salir.GetComponent<RectTransform>().localPosition=posicionCentral;
		Creditos.GetComponent<RectTransform>().localPosition=posicionSecundariaBaja;
		Nuevo_Juego.GetComponent<RectTransform>().localPosition=posicionTerciariaBaja;





		Texto_Continuar.GetComponent<Text>().color=alfaTerciario;
		Texto_Opciones.GetComponent<Text>().color=alfaSecundario;
		Texto_Salir.GetComponent<Text>().color=alfaCentral;
		Texto_Creditos.GetComponent<Text>().color=alfaSecundario;
		Texto_Nuevo_Juego.GetComponent<Text>().color=alfaTerciario;



		Continuar.GetComponent<Image>().color=alfaTerciarioB;
		Opciones.GetComponent<Image>().color=alfaSecundarioB;
		Salir.GetComponent<Image>().color=alfaCentralB;
		Creditos.GetComponent<Image>().color=alfaSecundarioB;
		Nuevo_Juego.GetComponent<Image>().color=alfaTerciarioB;



		brillo_Opciones.SetActive(false);
		brillo_Creditos.SetActive(false);
		brillo_Salir.SetActive(true);



		print("Estado 4");
	}

	void estado_Creditos(){

		Opciones.GetComponent<RectTransform>().localScale=tercerTamanio;
		Salir.GetComponent<RectTransform>().localScale=segundoTamanio;
		Creditos.GetComponent<RectTransform>().localScale=primerTamanio;
		Nuevo_Juego.GetComponent<RectTransform>().localScale=segundoTamanio;
		Continuar.GetComponent<RectTransform>().localScale=tercerTamanio;



		Opciones.GetComponent<RectTransform>().localPosition=posicionTerciaria;
		Salir.GetComponent<RectTransform>().localPosition=posicionSecundaria;
		Creditos.GetComponent<RectTransform>().localPosition=posicionCentral;
		Nuevo_Juego.GetComponent<RectTransform>().localPosition=posicionSecundariaBaja;
		Continuar.GetComponent<RectTransform>().localPosition=posicionTerciariaBaja;




		Texto_Opciones.GetComponent<Text>().color=alfaTerciario;
		Texto_Salir.GetComponent<Text>().color=alfaSecundario;
		Texto_Creditos.GetComponent<Text>().color=alfaCentral;
		Texto_Nuevo_Juego.GetComponent<Text>().color=alfaSecundario;
		Texto_Continuar.GetComponent<Text>().color=alfaTerciario;



		Opciones.GetComponent<Image>().color=alfaTerciarioB;
		Salir.GetComponent<Image>().color=alfaSecundarioB;
		Creditos.GetComponent<Image>().color=alfaCentralB;
		Nuevo_Juego.GetComponent<Image>().color=alfaSecundarioB;
		Continuar.GetComponent<Image>().color=alfaTerciarioB;


		brillo_NuevoJuego.SetActive(false);
		brillo_Creditos.SetActive(true);
		brillo_Salir.SetActive(false);



		print("Estado 5");
	}
}