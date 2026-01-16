using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class GameController : MonoBehaviour {

	public GameObject Vaisseau;

	private const int NOMBREVAISSEAUXALLIES = 10;
	private const int NOMBREVAISSEAUXENNEMIS = 10;

	private List<GameObject> VaisseauxEnnemis;
	private List<GameObject> VaisseauxAllies;

	// Use this for initialization
	void Start () {

		//Instianciation des variables
		VaisseauxAllies = new List<GameObject> ();
		VaisseauxEnnemis = new List<GameObject> ();

		initialisation_carte ();
		initialisation_vaisseaux (VaisseauxAllies, NOMBREVAISSEAUXALLIES, new Vector3 ());
		initialisation_vaisseaux (VaisseauxEnnemis, NOMBREVAISSEAUXENNEMIS, new Vector3 ());
		initialisation_drapeau ();
		initialisation_score ();

	}
	
	// Update is called once per frame
	void Update () {
	
	}


	private void initialisation_carte(){

	}



	/// <summary>
	/// Initialisation de l'ensemble des vaisseaux
	/// </summary>
	private void initialisation_vaisseaux(List<GameObject> ListeVaisseaux , int NombreDeVaisseaux, Vector3 PointDeDepart)
	{
		for (int i = 0; i < NombreDeVaisseaux; i++) {
			ListeVaisseaux.Add((GameObject)Instantiate(Vaisseau));
		}


	}

	private void initialisation_drapeau(){

	}

	private void initialisation_score(){
	}
}