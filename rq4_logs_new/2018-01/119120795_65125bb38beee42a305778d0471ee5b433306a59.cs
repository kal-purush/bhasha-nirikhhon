using UnityEngine;
using System.Collections;

public class Global : MonoBehaviourSingleton<Global> //acts almost like a bank for the global variables to be called later
{
	public static Global me;

	public bool beads = false; 
	public bool ring1 = false; 
	public bool ring2 = false; 
	public bool fishfood = false; 
	public bool mixtape = false;
	public bool perfume = false;
	public bool waterbottle = false;
	public bool certificate = false;

	private void Awake()
	{
		me = this; //awakens the script
	}
}