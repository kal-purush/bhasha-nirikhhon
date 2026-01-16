using UnityEngine;
using System.Collections.Generic;

public interface IHighlightable {
	void OnHighlight ();
	void OnDehighlight ();
}

public class Highlighter<T> : MonoBehaviour {
	public HashSet<T> sources;
	public IHighlightable target;
	public bool isHighlighted = false;
	public bool hasSources { get { return sources.Count > 0; } }

	void Start() {
		sources = new HashSet<T> ();
		target = GetComponent<IHighlightable> ();
	}

	protected void EmitIfChanged() {
		if (!isHighlighted && hasSources) {
			target.OnHighlight ();
		} else if (isHighlighted && !hasSources) {
			target.OnDehighlight ();
		}
		isHighlighted = hasSources;
	}

	public void On(T source) {
		sources.Add (source);
		EmitIfChanged ();
	}

	public void Off(T source) {
		sources.Remove (source);
		EmitIfChanged ();
	}

	public bool IsOn(T source) {
		return sources.Contains (source);
	}

	public bool Toggle(T source) {
		if (IsOn (source)) {
			Off (source);
		} else {
			On (source);
		}
		return IsOn (source);
	}
}