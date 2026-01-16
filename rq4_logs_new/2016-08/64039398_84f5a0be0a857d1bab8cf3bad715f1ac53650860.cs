using UnityEngine;
using System.Collections;

public class Respawn : MonoBehaviour {

	// Use this for initialization
	void Start () {
	    
	}
	
	// Update is called once per frame
	void Update () {
	
	}

    public void RespawnPlayer()
    {
        Vector3 resetPosition = Camera.main.ViewportToWorldPoint(new Vector3(0.5f, 0.5f, 0));
        resetPosition.z = 0;
        StartCoroutine(SpawnDelay(2, resetPosition));
    }

    IEnumerator SpawnDelay(int secondsToWait, Vector3 resetPosition)
    {
        yield return new WaitForSeconds(secondsToWait);
        transform.GetChild(0).gameObject.SetActive(true);
        transform.GetChild(0).position = resetPosition;
    }
}