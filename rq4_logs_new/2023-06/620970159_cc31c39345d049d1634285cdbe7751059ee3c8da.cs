using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
using UnityEngine.Networking;
using UnityEngine.SceneManagement;
using System;

public class PlayerToken // info returned by the request
{
    public int player_id;
    public string username;
}

[System.Serializable]
public class PlayerCredentials
{
    public string username;
    public string password;
}

public class ValidatePlayerCredentials : MonoBehaviour
{
    [SerializeField] TMP_InputField username;
    [SerializeField] TMP_InputField password;
    [SerializeField] string url;
    [SerializeField] string endpoint;
    [SerializeField] Message notification;

    public PlayerToken player; // player_id and username will be extracted to an instance of class PlayerToken

    public void QueryPlayer()
    {
        StartCoroutine(ValidateCredentials());
    }


    IEnumerator ValidateCredentials()
    {
        /*
        // This should work with an API that does NOT expect JSON
        WWWForm form = new WWWForm();
        form.AddField("name", "newGuy" + Random.Range(1000, 9000).ToString());
        form.AddField("surname", "Tester" + Random.Range(1000, 9000).ToString());
        Debug.Log(form);
        */

        // Create the object to be sent as json
        PlayerCredentials credentials = new PlayerCredentials();

        credentials.username = username.text;
        credentials.password = password.text;

        string playerCredentials = JsonUtility.ToJson(credentials);

        // Send using the Put method:
        // https://stackoverflow.com/questions/68156230/unitywebrequest-post-not-sending-body

        using (UnityWebRequest www = UnityWebRequest.Put(url + endpoint, playerCredentials))
        {

            www.method = "POST";
            www.SetRequestHeader("Content-Type", "application/json");
            yield return www.SendWebRequest();

            if (www.result == UnityWebRequest.Result.Success)
            {
                Debug.Log("Response: " + www.downloadHandler.text);

                player = JsonUtility.FromJson<PlayerToken>(www.downloadHandler.text); // player info

                // notify the player
                notification.Send($"Welcome {player.username}!");

                // save player info to PlayerPrefs

                PlayerPrefs.SetInt("player_id", player.player_id);


                // Fetch player information
               // QueryPlayerStatus(playerCredentials);

                yield return new WaitForSeconds(4);

                SceneManager.LoadScene("Main_menu");
            }
            else
            {
                Debug.Log("Error: " + www.error);
                string errorMessage = www.downloadHandler.text;
                notification.Send(errorMessage);
            }
        }
    }

    //private void QueryPlayerStatus(string playerCredentials)
    //{
    //    StartCoroutine(FetchPlayerInformation(playerCredentials));
    //}


    //IEnumerator FetchPlayerInformation(string playerCredentials)
    //{

    //}
}