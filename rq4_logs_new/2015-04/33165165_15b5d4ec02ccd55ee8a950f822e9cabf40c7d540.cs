using UnityEngine;
using System.Collections;

public class GridLines : MonoBehaviour
{
	public float spaceWidth = 32f;
	public float spaceHeight = 32f;
	public Color lineColor = Color.white;
	
	private float large = 10000f;
	
	public void OnDrawGizmos()
	{
		Vector3 pos = Camera.current.transform.position;
		Gizmos.color = lineColor;
		
		for(float y = pos.y - 800f; y < pos.y + 800f; y += spaceHeight)
		{
			Gizmos.DrawLine(new Vector3(-large, Mathf.Floor(y/spaceHeight) * spaceHeight, 0.0f),
			                new Vector3(large, Mathf.Floor(y/spaceHeight) * spaceHeight, 0.0f));
		}
		
        for(float y = pos.y - 800f; y < pos.y + 800f; y += spaceHeight)
        {
			Gizmos.DrawLine(new Vector3(-large, Mathf.Floor(y/spaceWidth) * spaceWidth, 0.0f),
				            new Vector3(large, Mathf.Floor(y/spaceWidth) * spaceWidth, 0.0f));
        }
	}
}