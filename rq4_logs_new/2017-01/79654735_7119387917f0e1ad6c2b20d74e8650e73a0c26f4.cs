using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Rocks : MonoBehaviour {
	// Inner radius, rocks do not appear inside this circle.
	public float InnerRadius;
	// Outer radius, rocks appear inside this circle.
	public float OuterRadius;
	// Distance between rocks.
	public float RockSpacing;
	// Object to spawn as rocks.
	public GameObject Rock;

	private float WorldToGrid;
	private int GridCount;
	private int[,] Grid;
	private List<Vector2> RockLocations;
	private const int MaxSamples = 10;

	void Start() {
		this.Rock.SetActive(false);
		this.Generate();
	}

	void Generate() {
		foreach (Transform child in this.transform) {
			Object.Destroy(child.gameObject);
		}

		this.WorldToGrid = Mathf.Sqrt(2) / this.RockSpacing;
		this.GridCount = Mathf.CeilToInt(2.0f * this.OuterRadius * this.WorldToGrid);

		this.Grid = new int[this.GridCount, this.GridCount];
		this.RockLocations = new List<Vector2>();

		Queue<Vector2> queue = new Queue<Vector2>();
		float angle = Random.Range(-Mathf.PI, Mathf.PI);
		this.TryAddRock(new Vector2(this.InnerRadius * Mathf.Cos(angle), this.InnerRadius * Mathf.Sin(angle)), queue);
		for (int i = 0; i < 10000 && queue.Count > 0; i++) {
			this.TryAddRock(queue.Dequeue(), queue);
		}

	}

	// Find the closest rock to the given point within the given radius.
	public bool FindRock(Vector2 center, float radius, out Vector2 rockPos) {
		int x0 = Mathf.FloorToInt((center.x + this.OuterRadius - this.RockSpacing) * this.WorldToGrid);
		int y0 = Mathf.FloorToInt((center.y + this.OuterRadius - this.RockSpacing) * this.WorldToGrid);
		int x1 = Mathf.CeilToInt((center.x + this.OuterRadius + this.RockSpacing) * this.WorldToGrid) + 1;
		int y1 = Mathf.CeilToInt((center.y + this.OuterRadius + this.RockSpacing) * this.WorldToGrid) + 1;
		x0 = Mathf.Max(0, x0);
		y0 = Mathf.Max(0, y0);
		x1 = Mathf.Min(this.GridCount, x1);
		y1 = Mathf.Min(this.GridCount, y1);
		float minR2 = radius * radius;
		bool found = false;
		rockPos = Vector2.zero;
		for (int xt = x0; xt < x1; xt++) {
			for (int yt = y0; yt < y1; yt++) {
				int idx = this.Grid[xt, yt];
				if (idx == 0) {
					continue;
				}
				Vector2 v = this.RockLocations[idx - 1];
				float dx = center.x - v.x, dy = center.y - v.y;
				float r2 = dx * dx + dy * dy;
				if (r2 < minR2) {
					found = true;
					minR2 = r2;
					rockPos = v;
				}
			}
		}
		return found;
	}

	void TryAddRock(Vector2 pos, Queue<Vector2> queue) {
		int xi = Mathf.FloorToInt((pos.x + this.OuterRadius) * this.WorldToGrid);
		int yi = Mathf.FloorToInt((pos.y + this.OuterRadius) * this.WorldToGrid);
		if (xi < 0 || xi >= this.GridCount || yi < 0 || yi >= this.GridCount) {
			return;
		}
		if (this.Grid[xi, yi] > 0) {
			return;
		}
		Vector2 rockPos;
		if (this.FindRock(pos, this.RockSpacing, out rockPos)) {
			return;
		}
		GameObject rock = Object.Instantiate(this.Rock, new Vector3(pos.x, pos.y), Quaternion.identity, this.transform);
		rock.SetActive(true);
		this.RockLocations.Add(pos);
		this.Grid[xi, yi] = this.RockLocations.Count;
		for (int i = 0; i < MaxSamples; i++) {
			float angle = Random.Range(-Mathf.PI, Mathf.PI);
			float d = this.RockSpacing * 1.01f;
			Vector2 npos = new Vector2(pos.x + Mathf.Cos(angle) * d, pos.y + Mathf.Sin(angle) * d);
			float r2 = npos.x * npos.x + npos.y * npos.y;
			if (r2 >= this.InnerRadius * this.InnerRadius && r2 <= this.OuterRadius * this.OuterRadius) {
				queue.Enqueue(npos);
			}
		}
	}
}