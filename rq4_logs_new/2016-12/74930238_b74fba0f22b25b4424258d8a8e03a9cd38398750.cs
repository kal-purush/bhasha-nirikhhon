using UnityEngine;
using System.Collections;

public class audio_MenuPrincipal : MonoBehaviour {

	/*Programado por José
		Este script sirve para modificar los niveles de volumen de los sonidos y de la música en el menu principal.
		Va a tomar como referencia los valores generales VOLUMEN_MUSICA y VOLUMEN_SFX encontrados en los playerprefs.
		Las funciones que realizarán esto serán publicas, para que el código menu_opciones pueda acceder a ellas y cambiar el volúmen.
	*/

	public AudioSource Musica_Tema;

	public AudioSource SFX_OK;
	public AudioSource SFX_Navegacion;
	public AudioSource SFX_Incorrecto;
	public AudioSource SFX_Comenzar;
	public AudioSource SFX_Tap;


	// Use this for initialization
	void Start () {
		ajustarVolumenMusica();
		ajustarVolumenSonido();
	}
	
	// Update is called once per frame
	void Update () {
	
	}



	public void ajustarVolumenMusica(){
		Musica_Tema.volume=PlayerPrefs.GetFloat("VOLUMEN_MUSICA");
	}


	public void ajustarVolumenSonido(){
		SFX_OK.volume=PlayerPrefs.GetFloat("VOLUMEN_SFX");
		SFX_Navegacion.volume=PlayerPrefs.GetFloat("VOLUMEN_SFX");
		SFX_Incorrecto.volume=PlayerPrefs.GetFloat("VOLUMEN_SFX");;
		SFX_Comenzar.volume=PlayerPrefs.GetFloat("VOLUMEN_SFX");;
		SFX_Tap.volume=PlayerPrefs.GetFloat("VOLUMEN_SFX");;
	}

}