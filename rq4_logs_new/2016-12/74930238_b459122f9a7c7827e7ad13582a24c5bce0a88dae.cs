using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class admin_seleccionarIdioma : MonoBehaviour {

	/*Programado por José
	 Este código es el administrador de la escena seleccionar_Idioma.
	 Detecta cuando se elige una de las dos opciones y cambia los player prefs segun la opción deseada.
 	 En cuanto se selecciona un idioma activa la cortinilla para ir a la siguiente escena, Historia_introducción.
 	 */

	public GameObject luzEnglish;
	public GameObject LuzEspaniol;

	public Text titulo;

	public Text English;
	public Text Espaniol;

	public AudioSource SFX_Seleccionar;
	public AudioSource SFX_Aceptar;

	string idiomaActual="English";


	// Use this for initialization
	void Start () {}


	// Update is called once per frame
	void Update () {
		if(Input.GetKeyDown(KeyCode.LeftArrow)||Input.GetKeyDown(KeyCode.A)||Input.GetKeyDown(KeyCode.W)||Input.GetKeyDown(KeyCode.UpArrow)){
			seleccionarIngles();
		}
		if(Input.GetKeyDown(KeyCode.RightArrow)||Input.GetKeyDown(KeyCode.D)||Input.GetKeyDown(KeyCode.S)||Input.GetKeyDown(KeyCode.DownArrow)){
			seleccionarEspaniol();
		}
	
		if(Input.GetKeyDown(KeyCode.Space)||Input.GetKeyDown(KeyCode.Return)){
			aceptarIdioma();
		}
	}



	public void seleccionarIngles(){
		SFX_Seleccionar.Play();
		luzEnglish.SetActive(true);
		LuzEspaniol.SetActive(false);
		Espaniol.fontSize=220;
		idiomaActual="ENGLISH";
		titulo.text="LANGUAGE";
	}


	public void seleccionarEspaniol(){
		SFX_Seleccionar.Play();
		LuzEspaniol.SetActive(true);
		luzEnglish.SetActive(false);
		English.fontSize=220;
		idiomaActual="ESPANIOL";
		titulo.text="IDIOMA";
	}



	public void aceptarIdioma(){
		SFX_Aceptar.Play();
		if(idiomaActual=="ENGLISH")
			print("The english lenguage has been selected.");
		if(idiomaActual=="ESPANIOL")
			print("El idioma español ha sido seleccionado.");
		

		PlayerPrefs.SetString("IDIOMA",idiomaActual);
		print("El idioma actual es: "+PlayerPrefs.GetString("IDIOMA"));
	}

}