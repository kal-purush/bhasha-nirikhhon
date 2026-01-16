using UnityEngine;
using System.Collections;

public class CampoJuego : MonoBehaviour {
    
    public bool invertido = false;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        if(invertido) {
            transform.eulerAngles = new Vector3(transform.eulerAngles.x, transform.eulerAngles.y, 180f);
            StartCoroutine(DesInvertir());
        }
        else {
            transform.eulerAngles = new Vector3(transform.eulerAngles.x, transform.eulerAngles.y, 0f);
        }
	
	}
    
    IEnumerator DesInvertir() {
        
        yield return new WaitForSeconds(5);
        
        invertido = false;
        
    }
}