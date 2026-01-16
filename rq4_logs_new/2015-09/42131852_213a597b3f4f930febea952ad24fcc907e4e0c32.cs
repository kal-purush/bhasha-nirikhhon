using UnityEngine;
using System.Collections;

[ExecuteInEditMode]
public class WorldScaler : MonoBehaviour
{
	private Transform gameCameraTfm;

	private Transform tfm;

	private void Start()
	{
		this.gameCameraTfm = Camera.main.transform;

		this.tfm = this.GetComponent<Transform>();
	}

	private void LateUpdate()
	{
		if (this.tfm != null)
		{
			this.tfm.localScale = new Vector3(1, 1 / Mathf.Cos(this.gameCameraTfm.eulerAngles.x * Mathf.Deg2Rad), 1 / Mathf.Sin(this.gameCameraTfm.eulerAngles.x * Mathf.Deg2Rad));
		}
	}
}