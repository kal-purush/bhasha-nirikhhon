using UnityEngine;
using System.Collections;

public class GraveRandomizerControl : MonoBehaviour {

    int winTomb;

    int GetEmptyGrave(GameObject[] tombs)
    {
        int r, tries = 200;
        do {
            r = (int)(Random.value * 10000) % tombs.Length;
            tries--;
        }
        while (tombs[r].GetComponent<graveControl>().empty == false && tries > 0);

        return r;
    }
	
	void Start () {

        GameObject[] tombs = GameObject.FindGameObjectsWithTag("graveArea");

        // Set win grave
        winTomb = GetEmptyGrave(tombs);
        tombs[winTomb].GetComponent<graveControl>().empty = false;
        tombs[winTomb].GetComponent<graveControl>().graveLoot = graveControl.loot.WIN;

    }
	

}