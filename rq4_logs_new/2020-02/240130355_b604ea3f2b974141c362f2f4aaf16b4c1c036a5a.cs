using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(MeshFilter), typeof(MeshRenderer))]
public class GridOfGrids : MonoBehaviour
{
	public int xSize, ySize, zSize;
	public float vRes;
	private Mesh mesh;

	private void Awake()
	{
		StartCoroutine(Generate());
		foreach (Vector3 v in vertices)
		{
			Debug.Log(v);
		}
		//xSize = xSize * vRes;
		//ySize = ySize * vRes;
		//zSize = zSize * vRes;
	}

	private Vector3[] vertices;

	private IEnumerator Generate()
	{
		vertices = new Vector3[(xSize) * (ySize) * (zSize)];
		GetComponent<MeshFilter>().mesh = mesh = new Mesh();
		mesh.name = "Procedural Grid";
		WaitForSeconds wait = new WaitForSeconds(0.01f);

		int vertGen = 0;
		for (float z = 0; z < zSize; z += vRes)
		{
			for (float y = 0; y < ySize; y += vRes)
			{
				for (float x = 0; x < xSize * vRes; x += vRes, vertGen++)
				{
					vertices[vertGen] = new Vector3(x, y, z);
				}
			}
		}
		mesh.vertices = vertices;
		int[] triangles = new int[6*(xSize-1)*(ySize-1)*(zSize)];
		for (int zInd = 0, totalTri = 0; zInd < ((zSize) * (ySize) * (xSize)); zInd += (xSize * ySize))
		{
			for (int yInd = 0; yInd < (ySize - 1) * (xSize - 1); yInd += (xSize))
			{
				for (int xInd = 0; xInd < xSize - 1; xInd++, totalTri += 6)
				{
					Debug.Log(xInd + yInd + zInd);
					triangles[totalTri]		= xInd + yInd + zInd + 0;
					triangles[totalTri + 1] = xInd + yInd + zInd + xSize;
					triangles[totalTri + 2] = xInd + yInd + zInd + 1;
					triangles[totalTri + 3] = xInd + yInd + zInd + 1;
					triangles[totalTri + 4] = xInd + yInd + zInd + xSize;
					triangles[totalTri + 5] = xInd + yInd + zInd + xSize + 1;
					yield return wait;
					mesh.triangles = triangles;
				}
			}
		}

		mesh.triangles = triangles;
	}

	private Color selectColor(float input)
	{
		return new Color(input, input, input);
	}

	public float DensityFxn(Vector3 input)
	{
		float a = PerlinNoise3D(input.x/10.0f, input.y/10.0f, input.z/10.0f);
		Debug.Log(a);
		return a;
	}

	private void OnDrawGizmos()
	{
		if (vertices == null)
		{
			return;
		}
		for (int i = 0; i < vertices.Length; i++)
		{
			//Gizmos.color = selectColor(DensityFxn(vertices[i]));
			Gizmos.DrawSphere(vertices[i], 0.1f);
		}
	}


	public static float PerlinNoise3D(float x, float y, float z)
	{
		y += 1;
		z += 2;
		float xy = _perlin3DFixed(x, y);
		float xz = _perlin3DFixed(x, z);
		float yz = _perlin3DFixed(y, z);
		float yx = _perlin3DFixed(y, x);
		float zx = _perlin3DFixed(z, x);
		float zy = _perlin3DFixed(z, y);
		return xy * xz * yz * yx * zx * zy;
	}
	static float _perlin3DFixed(float a, float b)
	{
		return Mathf.Sin(Mathf.PI * Mathf.PerlinNoise(a, b));
	}
}
