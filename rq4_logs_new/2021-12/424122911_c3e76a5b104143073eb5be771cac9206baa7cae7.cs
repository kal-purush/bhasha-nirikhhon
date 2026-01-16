using System;
using System.Collections.Generic;
using UnityEngine;

namespace Code.Scripts.Character
{
    public class CharacterManager : MonoBehaviour
    {
        private static CharacterManager _instance;

        public static CharacterManager Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = FindObjectOfType<CharacterManager>();
                    if (_instance == null)
                    {
                        Debug.LogError("Character Manager Not Found!");
                    }
                }

                return _instance;
            }
        }
        
        
        public List<CharacterInstance> characterInstances = new List<CharacterInstance>();

        private void Start()
        {
            characterInstances.AddRange(FindObjectsOfType<CharacterInstance>());
        }
    }
}