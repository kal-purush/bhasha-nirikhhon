using ReupVirtualTwin.helpers;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Networking;

public class TestChangeTextureFromAngular : MonoBehaviour
{
    public GameObject objectToChange;

    MaterialsManager _materialsManager;
    Texture2D texture;

    void Start()
    {
        _materialsManager = ObjectFinder.FindMaterialsManager().GetComponent<MaterialsManager>();
        _materialsManager.SelectObjects(new List<GameObject> { objectToChange }, new int[] { 0 });
    }
    public IEnumerator TestChangeTexture(string url)
    {
        yield return StartCoroutine(LoadTextureFromUrl(url));
        var material = new Material(Shader.Find("Universal Render Pipeline/Lit"));
        material.SetTexture("_BaseMap", texture);
        _materialsManager.SetNewMaterial(material);
    }

    IEnumerator LoadTextureFromUrl (string url) {
        var image = UnityWebRequestTexture.GetTexture(url);
        yield return image.SendWebRequest();
        if (image.result != UnityWebRequest.Result.Success)
        {
            Debug.LogError(image.error);
        }
        else
        {
            texture = DownloadHandlerTexture.GetContent(image);
        }
    }

}