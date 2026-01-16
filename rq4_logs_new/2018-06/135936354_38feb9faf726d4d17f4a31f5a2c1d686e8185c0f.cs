using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour {
    public static int PartyMax = 3;
    public static List<Character> Party;
	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}
    public static void RemoveFromParty(Character c)
    {
        if (Party == null)
        {
            Party = new List<Character>();
        }
        if (Party.Contains(c))
        {
            Party.Remove(c);
        }
    }
    public static void AddToParty(Character c)
    {
        if (Party == null)
        {
            Party = new List<Character>();
        }
        if (Party.Count < PartyMax)
        {
            Party.Add(c);
        }
    }

}