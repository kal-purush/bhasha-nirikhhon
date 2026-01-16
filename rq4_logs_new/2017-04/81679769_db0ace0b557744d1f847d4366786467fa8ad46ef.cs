using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using RAIN.Core;
using RAIN.Navigation;
using RAIN.Navigation.Waypoints;
using RAIN.BehaviorTrees;
public class AIScript : MonoBehaviour {
	private GameObject player;
	private AIRig rig;
	[System.Serializable]
	public class EventBooleans{
		public string boolName;
		public bool b;
	}

	[SerializeField]
	private List<EventBooleans> ;

	// Use this for initialization
	void Start () {
		
		rig = GetComponentInChildren<AIRig> ();
		rig.AI.WorkingMemory.SetItem<string> ("_route", "Waypoint Route");


//		mainRoute = NavigationManager.Instance.GetWaypointSet ("Waypoint Route");
//		otherRoute = NavigationManager.Instance.GetWaypointSet ("OtherRoute");
//		rig.AI.WorkingMemory.SetItem<WaypointSet> ("_route", mainRoute);
//		Debug.Log(rig.AI.WorkingMemory.GetItem<GameObject>("_route").ToString());
	}
	
	// Update is called once per frame
	void Update () {
		if (isTrue("triggered")) {
			List<string> items = rig.AI.WorkingMemory.GetItems ();
			foreach (string item in items) {
				Debug.Log (item);
			}
			changeRoute ("OtherRoute");
		} else if(isFalse("triggered")){
			changeRoute ("Waypoint Route");
		}
	}

	private void changeRoute (string route){
		rig.AI.WorkingMemory.SetItem<string> ("_route", route);
		rig.AI.Navigator.RestartPathfindingSearch ();
	}

	private bool isTrue (string memoryVariableName){
		return rig.AI.WorkingMemory.GetItem<bool> (memoryVariableName);
	}

	private bool isFalse (string memoryVariableName){
		return !rig.AI.WorkingMemory.GetItem<bool> (memoryVariableName);
	}

	private bool getBoolean(string name){
		return rig.AI.WorkingMemory.GetItem<bool> (name);
	}
}