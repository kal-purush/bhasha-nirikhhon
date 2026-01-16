using UnityEngine;
using System.Collections;

public class Paycheck : MonoBehaviour {
	public int progress;
	public float cash;

	private ThingController thing;

	void Start () {
		thing = GetComponentInParent<ThingController> ();
		thing.ProgressChanged += OnProgress;
	}

	void OnProgress(object sender, ProgressEventArgs e) {
		if (e.before < this.progress && e.after >= this.progress) {
			Debug.LogFormat("{0} progress went from {1} to {2} which crosses threshold {3}, adding {4} to cash",
			                thing.name, e.before, e.after, this.progress, this.cash);
			MoniesController.Instance.cash += this.cash;
			thing.ProgressChanged -= OnProgress;
		}	
	}
}