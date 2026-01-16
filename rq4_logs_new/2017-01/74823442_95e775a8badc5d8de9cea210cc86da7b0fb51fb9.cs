using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public partial class VillagerScript
{
	public bool canGoInside(GameObject container)
	{
		return Vector3.Distance(transform.position, container.transform.position) < 10;
	}

	public void GoInside(GameObject container)
	{
		if (canGoInside(container))
			Container = container;
	}

	public void GoOutside(Vector3 direction)
	{
		var pos = Container.transform.position + direction.normalized * 10;
		pos.y += 2;
		Container = null;
		transform.position = pos;
	}

	public void GoTowards(Vector2 target2D)
	{
		var pos2D = new Vector2(transform.position.x, transform.position.z);
		var diff2D = target2D - pos2D;
		var clamped = Vector2.ClampMagnitude(diff2D, Speed / 50.0f);

		var target3D = new Vector3(transform.position.x + clamped.x, 0, transform.position.z + clamped.y);
		target3D.y = Terrain.activeTerrain.SampleHeight(target3D);
		transform.position = target3D;

		transform.rotation = Quaternion.identity;
		transform.Rotate(0, Vector2.Angle(Vector2.up, diff2D), 0);
	}
}