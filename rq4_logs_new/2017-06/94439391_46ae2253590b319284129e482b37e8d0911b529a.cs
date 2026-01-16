using System.Collections;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Networking;

public class LoginManager : MonoBehaviour {

	//public string token = "b3601abb8df2e1b2f128c03c148e221dfb89e1cf";
	//public string username = "tammiet";
	public string username = "";
	public string token = "";
	public string avatar_url;
	public string total_private_repos;
	public Image myImage;
	public GameObject username_prompt;
	public GameObject token_prompt;

	// Use this for initialization
	void Start () 
	{
		username_prompt.SetActive (true);
		token_prompt.SetActive (false);
	}

	public void SubmitUsername()
	{
		if (Input.GetButtonDown ("Submit")) {
			if (username_prompt.GetComponent<InputField> ().text != "") 
			{
				username = username_prompt.GetComponent<InputField> ().text;
				ShowTokenPrompt ();
			}
		}
	}

	public void SubmitToken()
	{
		if (Input.GetButtonDown ("Submit")) {
			token = token_prompt.GetComponent<InputField> ().text;
			AuthUser (username, token);
		}
	}

	public void ShowUsernamePrompt()
	{
		username_prompt.SetActive (true);
		token_prompt.SetActive (false);
	}

	public void ShowTokenPrompt()
	{
		token_prompt.SetActive (true);
		username_prompt.SetActive (false);
	}

	public void AuthUser(string un, string tk)
	{
		string url_attribute = "https://api.github.com/users/" + un;
		StartCoroutine(GetJSON(url_attribute));
	}	

	IEnumerator GetJSON(string url)
	{
		UnityWebRequest www = UnityWebRequest.Get(url);

		yield return www.Send();

		if (www.isError)
		{
			Debug.Log(www.error);
		}
		else
		{
			//logs json from Github
			Debug.Log(www.downloadHandler.text);
			JsonUtility.FromJsonOverwrite(www.downloadHandler.text, this);
			string total_private_repos = (this.total_private_repos);
			Debug.Log(total_private_repos);

			//gets avatar_url and grabs image from web
			string image_url = (this.avatar_url);
			var www_image = new WWW(image_url);
			yield return www_image; // waits until image is downloaded
			//display image as a UI texture
			Texture2D texture = new Texture2D(1, 1);
			www_image.LoadImageIntoTexture(texture);
			Sprite sprite = Sprite.Create(texture, new Rect(0, 0, texture.width, texture.height), Vector2.one / 2);
			GetComponent<SpriteRenderer>().sprite = sprite;
		}
	}
}