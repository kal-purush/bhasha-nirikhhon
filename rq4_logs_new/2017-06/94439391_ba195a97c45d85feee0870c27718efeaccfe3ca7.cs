using System.Collections;
using UnityEngine;
using UnityEngine.Networking;

public class LoginValidation : MonoBehaviour {

    public string token = "b3601abb8df2e1b2f128c03c148e221dfb89e1cf";
    public string username = "tammiet";

    // Use this for initialization
    void Start () {
        string url_attribute = "https://api.github.com/users/" + username;
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
            Debug.Log(www.downloadHandler.text);
        }
    }
}