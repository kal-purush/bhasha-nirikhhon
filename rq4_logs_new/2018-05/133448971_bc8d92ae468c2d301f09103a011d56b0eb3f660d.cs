using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Boulder : MonoBehaviour {

	float move = 0;
	float speedFactor = 0.05f;
	void Start () {
		Color startColor = new Color( Random.value, Random.value, Random.value, 1.0f );
		Renderer rend = GetComponent<Renderer>();
		startColor.a = 0.9f;
		rend.material.color = startColor;	

	}
	// Update is called once per frame
	void Update () {
		if(move < 1)
		{
			move += Time.deltaTime * speedFactor;// whatever you want the speed to be

			if(move>1)
				move = 1;

			transform.position = new Vector3(Mathf.Lerp(this.gameObject.transform.position.x,0.0f,move),Mathf.Lerp(this.gameObject.transform.position.y,0.0f, move),Mathf.Lerp(this.gameObject.transform.position.z,0.0f,move));
		}
	}


	void OnMouseDown(){
		Destroy (this.gameObject);

	}
}