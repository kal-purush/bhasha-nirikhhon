using System;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace CleanArchitecture_Example.Scripts.InterfaceAdapters
{
    [CreateAssetMenu(fileName = "ImagesRepository", menuName = "ScriptableObjects/Images Repository", order = 0)]
    public class ImagesRepositoryScriptableObject : ScriptableObject
    {
        [Serializable]
        private class ImageDictionary
        {
            public Image Image;
            public string Key;
        }

        [SerializeField] private List<ImageDictionary> _images;

        public Image GetImage(string key)
        {
            return _images.Find(image => image.Key == key)?.Image;
        }
    }
}