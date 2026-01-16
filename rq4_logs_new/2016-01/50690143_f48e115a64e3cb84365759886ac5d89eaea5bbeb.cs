using UnityEngine;
using System.Collections;

public class KillAfter5sec : MonoBehaviour {

	// Use this for initialization
	void Start () {
        StartCoroutine(MyCoroutine());
	}

    IEnumerator MyCoroutine()
    {
        yield return new WaitForSeconds(5);
        Destroy(this.gameObject);
    }

    // Update is called once per frame
    void Update () {
	
	}
}