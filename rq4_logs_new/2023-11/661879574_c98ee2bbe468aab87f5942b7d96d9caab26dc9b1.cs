using System.Collections.Generic;
using UnityEngine;
using System;
using ReupVirtualTwin.models;

namespace ReupVirtualTwin.behaviours
{
    public class ObjectRegistry : MonoBehaviour
    {
        public List<GameObject> objects = new List<GameObject>();

        public void AddTree(GameObject parent)
        {
            if (parent.GetComponent<UniqueIdentifer>() == null)
            {
                throw new Exception($"object {parent.name} does not have a uniqueId component");
            }

            objects.Add(parent);

            foreach (Transform child in parent.transform)
            {
                AddTree(child.gameObject);
            }
        }

        public GameObject GetObjectWithGuid(string guid)
        {
            foreach (GameObject obj in objects)
            {
                var uniqueIdentifier = obj.GetComponent<UniqueIdentifer>();
                if (uniqueIdentifier.isIdCorrect(guid))
                {
                    return obj;
                }
            }
            return null;
        }

    }
}