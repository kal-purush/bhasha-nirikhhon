using UnityEngine;
using System.Collections;

public class Animate : MonoBehaviour {
	public static Animate animator;
	public bool Complete {
		get {return complete;}
	}

	private Transform[] positions = new Transform[2];
	private Rigidbody2D[] rbodies = new Rigidbody2D[2];
	private Animator[] animators = new Animator[2];
	private int maxCharacters = 2;
	private bool complete = false;

	void Awake() {
		if(animator == null) {
			DontDestroyOnLoad(gameObject);
			animator = this;
		} else if(animator != this) {
			Destroy(gameObject);
		}
	}

	void Start() {
		GameObject go;

		go = GameObject.Find("Luke");
		rbodies[0] = go.GetComponent<Rigidbody2D>();
		positions[0] = go.GetComponent<Transform>();;
		animators[0] = go.GetComponent<Animator>();

		go = GameObject.Find("Dan");
		positions[1] = go.GetComponent<Transform>();
		animators[1] = go.GetComponent<Animator>();
	}

	//where i = character number; 0 = luke, 1 = dan, etc.
	public void MoveToPosition(int i, Transform pos) {
		Debug.Log("Entered animate.");
		complete = false;

		if(i >= maxCharacters) {
			Debug.Log("Character does not exist!");
			return;
		}

		animators[i].SetBool("IsWalking", true);

		Debug.Log("Start Coroutine");
		StartCoroutine(Move(positions[i], positions[i].position , pos.position, 3.0f));
		Debug.Log("End of animate.");
	}

	IEnumerator Move(Transform pos, Vector2 startPosition, Vector2 endPosition, float time) {
		Debug.Log("Entered Move Coroutine");
		float loopTime = 0;
		float i = 0.0f;
		float rate = 1.0f/time;

		while(i < 1.0f) {
			i += Time.deltaTime * rate;
			pos.position = Vector2.Lerp(startPosition, endPosition, i);

			if(loopTime >= 2) {
				Debug.Log("infinite loop");
				break;
			}

			yield return null;
		}

		complete = true;
		Debug.Log("End of Move Coroutine");
	}  
}