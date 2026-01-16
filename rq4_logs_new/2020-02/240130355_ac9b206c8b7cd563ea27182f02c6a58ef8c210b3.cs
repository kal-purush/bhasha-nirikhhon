using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[SelectionBase]
public class VoxelGrid : MonoBehaviour
{
	public GameObject voxelPrefab;
	public int resolution;

	private bool[] voxels;
	private float voxelSize;

	public void Initialize(int resolution, float size)
	{
		this.resolution = resolution;
		voxelSize = size / resolution;
		voxels = new bool[resolution * resolution];

		for (int i = 0, y = 0; y < resolution; y++)
		{
			for (int x = 0; x < resolution; x++, i++)
			{
				CreateVoxel(i, x, y);
			}
		}
	}

	private void CreateVoxel(int i, int x, int y)
	{
		GameObject newVoxel = Instantiate(voxelPrefab) as GameObject;
		newVoxel.transform.parent = transform;
		newVoxel.transform.localPosition = new Vector3((x + 0.5f) * voxelSize, (y + 0.5f) * voxelSize);
		newVoxel.transform.localScale = Vector3.one * voxelSize;

		Vector3 voxelScale = Vector3.one * voxelSize * 0.9f;
		newVoxel.transform.localScale = voxelScale;
	}
}