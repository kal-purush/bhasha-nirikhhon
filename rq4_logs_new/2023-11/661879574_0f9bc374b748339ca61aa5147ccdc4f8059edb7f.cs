using ReupVirtualTwin.dataModels;
using ReupVirtualTwin.helpers;
using ReupVirtualTwin.models;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Networking;

namespace ReupVirtualTwin.behaviours
{
    public class SetMaterialRequestsReceiver : MonoBehaviour
    {
        Texture2D texture;
        Material material;
        List<GameObject> objects;

        IRegistry registry;

        private void Start()
        {
            registry = ObjectFinder.FindObjectRegistry().GetComponent<IRegistry>();
        }

        public IEnumerator ReceiveSetMaterialRequest(string serializedRequest)
        {
            SetMaterialRequest request = JsonUtility.FromJson<SetMaterialRequest>(serializedRequest);
            yield return StartCoroutine(LoadTextureFromUrl(request.texturesUrl));
            CreateMaterialWithTexture();
            FindObjects(request.objectsIds);
            MaterialsHelper.SetNewMaterialToObjects(objects, request.submeshIndexes, material);
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
        void CreateMaterialWithTexture()
        {
            material = new Material(Shader.Find("Universal Render Pipeline/Lit"));
            material.SetTexture("_BaseMap", texture);
        }
        void FindObjects(string[] ids)
        {
            objects = registry.GetItemsWithGuids(ids);
        }
    }
}