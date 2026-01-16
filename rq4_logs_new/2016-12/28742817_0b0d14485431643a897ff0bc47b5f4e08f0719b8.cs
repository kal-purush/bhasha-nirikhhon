using UnityEngine;
using System.Collections;

public class ParticleDeposition : MonoBehaviour {

	private TerrainData myTData;

	public int NumberOfPiles = 5;

	// Use this for initialization
	void Start () {
		// Particle Deposition Method from Game Programming Gems 1
		// Page 508

		// Instead of applying this to a flattened grid, let's 
		// first generate a random terrain with random values,
		// and then start depositing particles. 
		// That means, I'll copy and paste from RandomTerrain.cs

		myTData = Terrain.activeTerrain.terrainData;
		int y = myTData.heightmapHeight;
		int x = myTData.heightmapWidth;
		float[,] data = new float[y, x];
		float[,] temp = new float[y, x];
		for (int i = 0; i < y; i++) {
			for (int j = 0; j < x; j++) {
				Random.InitState (j + (j * i));
				float r = Random.value;
				if (r <= 0.25)
					r = 0.0f;
				else if (r <= 0.5)
					r = 0.0025f;
				else if (r <= 0.75)
					r = 0.0050f;
				else
					r = 0.0075f;
				data [i, j] = r;
				temp [i, j] = r;
			}
		}
		myTData.SetHeights (0, 0, data);
		GaussianSmooth s = new GaussianSmooth ();
		s.Smooth (myTData);

		// Now we can start the particle deposition. 
		for (int i = 0; i < NumberOfPiles; i++) {
			
		}

	}
	
	// Update is called once per frame
	void Update () {
	
	}
}