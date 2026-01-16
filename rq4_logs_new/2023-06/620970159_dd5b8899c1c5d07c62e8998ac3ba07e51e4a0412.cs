using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Networking;

public class InsertWeapon : MonoBehaviour
{
    [SerializeField] string url;
    [SerializeField] string endpoint;

    public void QueryAddWeapon(string data)
    {
        StartCoroutine(AddWeapon(data));
    }


    IEnumerator AddWeapon(string data)
    {
        using (UnityWebRequest www = UnityWebRequest.Put(url + endpoint, data))
        {

            www.method = "POST";
            www.SetRequestHeader("Content-Type", "application/json");
            yield return www.SendWebRequest();

            if (www.result == UnityWebRequest.Result.Success)
            {
                Debug.Log(UnityWebRequest.Result.Success);
            }
            else
            {
                Debug.Log("Error: " + www.error);
                string errorMessage = www.downloadHandler.text;
                //notification.Send(errorMessage);
            }
        }
    }
}