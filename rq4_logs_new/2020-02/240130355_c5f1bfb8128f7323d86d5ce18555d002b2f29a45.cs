using UnityEngine;
using System;

[Serializable]
public class Voxel
{
	public bool state;
	public Vector3 position, xEdgePosition, yEdgePosition, zEdgePosition;

	public Voxel(int x, int y, int z, float size)
	{
		position.x = (x + 0.5f) * size;
		position.y = (y + 0.5f) * size;
		position.z = (z + 0.5f) * size;

		float halfSize = size * 0.5f;
		xEdgePosition = yEdgePosition = zEdgePosition = position;
		xEdgePosition.x += halfSize;
		yEdgePosition.y += halfSize;
		zEdgePosition.z += halfSize;
	}

	//empty fallback initializer
	public Voxel() { }


	/*
	 * the following fxns could probably be turned into one big fxn
	 * call it BecomeDummyOf(Voxel voxel, float offset, Vector3 xyz)
	 * if xyz = (1,0,0) then do x dummy;
	 *			(0,1,0) is y dummy;
	 *			(1,1,1) is xyzdummy; etc
	 * just build everything out of switch()
	 */

	//edit for 3d
	public void BecomeXDummyOf(Voxel voxel, float offset)
	{
		state = voxel.state;
		position = voxel.position;
		xEdgePosition = voxel.xEdgePosition;
		yEdgePosition = voxel.yEdgePosition;
		position.x += offset;
		xEdgePosition.x += offset;
		yEdgePosition.x += offset;
	}

	//edit for 3d
	public void BecomeYDummyOf(Voxel voxel, float offset)
	{
		state = voxel.state;
		position = voxel.position;
		xEdgePosition = voxel.xEdgePosition;
		yEdgePosition = voxel.yEdgePosition;
		position.y += offset;
		xEdgePosition.y += offset;
		yEdgePosition.y += offset;
	}
	//build this
	public void BecomeZDummyOf(Voxel voxel, float offset)
	{
		state = voxel.state;
		position = voxel.position;
	}
	//edit for 3d
	public void BecomeXYDummyOf(Voxel voxel, float offset)
	{
		state = voxel.state;
		position = voxel.position;
		xEdgePosition = voxel.xEdgePosition;
		yEdgePosition = voxel.yEdgePosition;
		position.x += offset;
		position.y += offset;
		xEdgePosition.x += offset;
		xEdgePosition.y += offset;
		yEdgePosition.x += offset;
		yEdgePosition.y += offset;
	}
	//build this
	public void BecomeXZDummyOf(Voxel voxel, float offset)
	{
		state = voxel.state;
		position = voxel.position;
	}
	//build this
	public void BecomeYZDummyOf(Voxel voxel, float offset)
	{
		state = voxel.state;
		position = voxel.position;
	}
	//build this
	public void BecomeXYZDummyOf(Voxel voxel, float offset)
	{
		state = voxel.state;
		position = voxel.position;
	}
}
