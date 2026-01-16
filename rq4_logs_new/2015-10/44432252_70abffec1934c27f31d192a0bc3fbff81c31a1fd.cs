using UnityEngine;
using System.Collections;

public class LevelChangerController : MonoBehaviour {

	public Sprite onSprite;
	public Sprite offSprite;
	public GameObject[] associatedSwitches;
	public bool canSwitch;
	public string levelName;
	
	// Use this for initialization
	void Start ()
	{
		if (associatedSwitches != null)
		{
			foreach (var levelSwitch in associatedSwitches)
			{
				this.drawLineTo(levelSwitch.transform.position);
			}
		}
	}
	
	void OnMouseEnter() 
	{
		if (this.canSwitch == true)
		{
			this.setSprite(onSprite);
		}
	}
	
	void OnMouseExit()
	{
		this.setSprite(offSprite);
	}	
	
	void OnMouseDown()
	{
		if (this.canSwitch == true) 
		{
			Application.LoadLevel(levelName);	
		}
	}
	
	// Update is called once per frame
	void Update () {
	
	}
	
	private void setSprite(Sprite sprite) 
	{
		this.GetComponent<SpriteRenderer>().sprite = sprite;
	}
	
	private void drawLineTo(Vector3 endPoint)
	{
		var lineRenderer = this.gameObject.GetComponent<LineRenderer>();
		lineRenderer.SetWidth(0.05f, 0.05f);
		lineRenderer.SetVertexCount(2);
		lineRenderer.SetPosition(0, this.transform.position);
		lineRenderer.SetPosition(1, endPoint);
	}
}