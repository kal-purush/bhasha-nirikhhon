using UnityEngine;
using System;
using System.Collections;
using System.Collections.Generic;

public class PipeArrow : MonoBehaviour {

	private float speed = 20f;

	private Coroutine mouvementCoroutine = null;

	private SpriteRenderer spriteRenderer;

	public void Awake(){
		spriteRenderer = this.GetComponent<SpriteRenderer> ();
	}

	public void MoveToTarget(Vector3 target, Action<PipeArrow> onMoveFinishCallback){
		if (mouvementCoroutine != null) {
			StopCoroutine (mouvementCoroutine);
		}
		mouvementCoroutine = StartCoroutine(Movement(target, onMoveFinishCallback));
	}

	
	IEnumerator Movement (Vector3 target, Action<PipeArrow> onMoveFinishCallback) {

		while (Vector3.Distance(transform.position, target) > 0.05f) {
			transform.position = Vector3.MoveTowards(transform.position, target, speed*Time.deltaTime);
			yield return null;
		}

		onMoveFinishCallback (this);
	}

	public void SetHighLight(bool highlight){
		//print ("High");
		Color c = spriteRenderer.color;
		if (highlight) {
			speed = 60f;
			c.a = 200F/255F;
		}else{
			speed = 20f;
			c.a = 60F/255F;
		}
		spriteRenderer.color = c;
	}
}