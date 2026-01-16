using UnityEngine;
using System.Collections;

public class AnimationManager : MonoBehaviour {
	public struct Roll{
		public AnimationCurve collider_height;
		public AnimationCurve collider_center;
		public string ac_name;
		public Roll(string name){
			this.ac_name = name;

			Keyframe[] curve_keys = new Keyframe[]{
				new Keyframe(0f,1.702f),
				new Keyframe(0.1617759f,0.8004f),
				new Keyframe(0.3493693f,0.7f),
				new Keyframe(0.75f,0.7f),
				new Keyframe(0.847771f,0.8f),
				new Keyframe(1.066936f,1.702131f)
			};

			Keyframe[] curve_keys2 = new Keyframe[]{
				new Keyframe(0f,0.85f),
				new Keyframe(0.1617759f,0.4f),
				new Keyframe(0.3493693f,0.35f),
				new Keyframe(0.75f,0.35f),
				new Keyframe(0.847771f,0.4f),
				new Keyframe(1.066936f,0.85f)
			};
			this.collider_height = new AnimationCurve(curve_keys);
			this.collider_center = new AnimationCurve(curve_keys2);
		}
	}

	public float roll_timer { get; set; }
	public Roll roll = new Roll ("Roll");

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}