using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System;
using UnityEngine;

public class BirdAreasManager : MonoBehaviour {

/// TODO : Passer le BirdAreasManager (et donc tous les birds area) sur un Layer qui ne se fait pas render par la camera.
/// On varra toujours en mode Dev' les zones violettes qui permettent de bien placer les zones sans que le joueur ne les voient distinctement.

	private static BirdAreasManager instance; 
	public static BirdAreasManager Instance {get {return instance;} }

	[SerializeField]
	private GameObject birdGroupPrefab;
	[SerializeField]
	private List<BirdAreaBehaviour> areasAvailable;
	[SerializeField]
	private int initGroupBirdNumber;
	
	private List<BirdAreaBehaviour> areasNotAvailable = new List<BirdAreaBehaviour>();
	
	private void Awake() {
		if(instance != null && instance != this)
			Destroy(this.gameObject);
		else
			instance = this;
	}
	
	private void Start() {
		for(int i = 0; i < initGroupBirdNumber; i++) {
			Instantiate(birdGroupPrefab);
		}
		
		///TODO : TRES IMPORTANT POUR LA SUITE ! 
		/// Le nombre de BirdAreaBehaviour ne doit jamais être inférieur à nbBirds + NbJoueurs.
		/// Car si tous les joueurs se placent sur un spot et que chaque groupe d'oiseau est posé, alors si un groupe décolle, il ne trouvera pas de spot dispo.
		/// Il sera donc primordial d'effectuer ce test a l'aide d'un game manager qui connait NBJoueurs
		// if(GameManager.Instance.GetNbPlayers() + initGroupBirdNumber <= areasAvailable.Count + areasNotAvailable.Count)
		//	Debug.LogError("Erreur ! Pas de zone de landing disponibles. Vous devez placer plus de zones.");
	}
	
	public BirdAreaBehaviour BirdsLookingForArea(BirdAreaBehaviour previousArea) {		
		if(areasAvailable.Count <= 0) {
			Debug.LogError("Erreur ! Pas de zones de landing disponibles. Vous devez placer plus de zones.");
			return previousArea;
		}
		
		int rand = MathHelper.GetRandInRangeInt(0,areasAvailable.Count-1);
		BirdAreaBehaviour selectedArea = areasAvailable.ElementAt(rand);
		areasNotAvailable.Add(selectedArea);
		areasAvailable.RemoveAt(rand);
		
		if(previousArea != null) 
			areasAvailable.Add(previousArea);
		
		return(selectedArea);
	}
	
	public static BirdAreasManager GetInstance() {
		return instance;
	}
}