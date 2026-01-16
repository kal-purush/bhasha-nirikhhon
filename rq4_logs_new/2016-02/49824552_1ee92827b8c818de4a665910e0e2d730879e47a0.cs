using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using RTS;

public abstract class GenericRace : MonoBehaviour {

	// List of units and buildings that the race can create
	private List<string> unitsList;
	private List<string> buildingsList;



	/*** ------------------------------------------------------ ***/
	/*** 					Getters/Setters  					***/
	/*** ------------------------------------------------------ ***/

	public List<string> getUnitsList() { return unitsList; }
	public List<string> getBuildingsList() { return buildingsList; }
}