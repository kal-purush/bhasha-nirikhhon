using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[SelectionBase]
public class PixelGrid : MonoBehaviour
{
	public GameObject pixelPrefab;
	public int resolution;

	//a pixel does or doesn't exist -- hence, bool array.
	private bool[] pixels;
	private float pixelSize;


	private Material[] pixelMaterials; //paint code

	//this function takes some resolution (how many cubes in this chunk), and some size (the size of the cubes)
	public void Initialize(int resolution, float size)
	{
		this.resolution = resolution;
		pixelSize = size / resolution;

		//this generates an array of size resolution^2
		//would want res^3 in 3D
		pixels = new bool[resolution * resolution];


		pixelMaterials = new Material[pixels.Length]; //paint code

		//iterate over all x,y creating pixels
		for (int i = 0, y = 0; y < resolution; y++)
		{
			for (int x = 0; x < resolution; x++, i++)
			{
				CreatePixel(i, x, y);
			}
		}

		SetPixelColors(); //paint code
	}

	//i'm not going to bother explaining this -- check the createPixel in pixelMap
	private void CreatePixel(int i, int x, int y)
	{
		GameObject newPixel = Instantiate(pixelPrefab) as GameObject;
		newPixel.transform.parent = transform;
		newPixel.transform.localPosition = new Vector3((x + 0.5f) * pixelSize, (y + 0.5f) * pixelSize, -0.01f);
		newPixel.transform.localScale = Vector3.one * pixelSize * 0.1f;
		pixelMaterials[i] = newPixel.GetComponent<MeshRenderer>().material; //paintCode;
	}


	public void SetPixel(int x, int y, bool state) //paint code
	{
		pixels[y * resolution + x] = state;
		SetPixelColors();
	}

	private void SetPixelColors() //paint code
	{
		for (int i = 0; i < pixels.Length; i++)
		{
			pixelMaterials[i].color = pixels[i] ? Color.black : Color.white;
		}
	}
}