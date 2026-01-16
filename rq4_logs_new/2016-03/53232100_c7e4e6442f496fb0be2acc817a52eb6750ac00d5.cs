using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

public class Button : MonoBehaviour 
{
	public enum ButtonAction {GoToScene, GoToLevel}
	public ButtonAction buttonAction;
	public int parm;

	const float scaleValue = 0.9f;

	void OnMouseDown()
	{
		this.transform.localScale *= scaleValue;
	}

	void OnMouseUp()
	{
		this.transform.localScale /= scaleValue;
		DoAction();
	}
		
	void DoAction()
	{
		switch (buttonAction)
		{
		case ButtonAction.GoToScene:
			SceneManager.LoadScene(parm);
			break;
		case ButtonAction.GoToLevel:
			SaveManager.Instance.SetCurrentLevel(parm);
			SceneManager.LoadScene(2);
			break;
		default:
			Debug.LogWarning(buttonAction.ToString());
			break;
		}
	}

	public void Disable()
	{
		TextMesh txt = this.transform.GetChild(0).GetComponent<TextMesh>();
		txt.color = new Color(txt.color.r, txt.color.g, txt.color.b, 0.5f);
		this.GetComponent<BoxCollider2D>().enabled = false;
	}
	public void Enable()
	{
		TextMesh txt = this.transform.GetChild(0).GetComponent<TextMesh>();
		txt.color = new Color(txt.color.r, txt.color.g, txt.color.b, 1);
		this.GetComponent<BoxCollider2D>().enabled = true;
	}
}