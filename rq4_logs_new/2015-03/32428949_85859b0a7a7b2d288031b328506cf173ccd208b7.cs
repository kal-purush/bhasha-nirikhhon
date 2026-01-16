using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class ButtonLogin : MonoBehaviour {

	public bool login_button = false;
	// make the email public and static to be accessed from other scripts
	public static string user_email;

	[SerializeField] private InputField _emailField;
	[SerializeField] private InputField _passField;
	

	void OnMouseUp() {
		if (login_button == true) {
			// if the user click login , call a function to start https request and 
			// authenticate the user
			StartCoroutine(user_login());
		}
	}





	IEnumerator user_login() {

		//get the user email and password
		string email = _emailField.text;
		string password = _passField.text;
		user_email = email;
		Debug.Log (email);
		Debug.Log (password);
		// we use cloud9 for now, this will change when deploying on heroko
		string urlMessage = "https://k12-mariammohamed.c9.io/api/records/user_login";
		WWWForm form = new WWWForm ();
		// pass the email and passsword for authentication
		form.AddField ("email", email);
		form.AddField ("password", password);
		WWW w = new WWW(urlMessage, form);
		yield return w;
		if (!string.IsNullOrEmpty (w.error)) {
			// this is done if the authentication is rejected or the response has
			// value >= 400 which means error in authentication or connection or server is down
			Debug.Log (w.error);
			_emailField.text = "";
			_passField.text = "";

		} else {
			// if the response has OK status, start the scene called "scene"
			Application.LoadLevel("scene");
		}

	}



}