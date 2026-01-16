using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class MenuController : MonoBehaviour {

	[Header("Input fields.")]
	public InputField hostInputField;
	public InputField portInputField;

	[Header("Buttons")]
	public Button startButton;

	private Client clientInstance
	{
		get
		{
			return Client.Instance;
		}
	}

	private void Start()
	{
		hostInputField.text = clientInstance.address;
		portInputField.text = clientInstance.port.ToString();
	}


	public void OnStartButtonClicked()
	{
		int parsedPort = 0;

		if (!int.TryParse(portInputField.text, out parsedPort))
		{
			Debug.LogError("Unable to parse port " + portInputField.text);
			return;
		}
		

		clientInstance.address = hostInputField.text;
		clientInstance.port = parsedPort;

		SceneManager.LoadScene("Intersection");
	}
}