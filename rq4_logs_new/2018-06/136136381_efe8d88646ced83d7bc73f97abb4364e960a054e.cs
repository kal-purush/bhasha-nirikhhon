using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Girigiri.Input
{
    public class InputManager : MonoBehaviour
    {
        public static InputManager Instance { get; private set; }
        public InputState State { get; private set; }
        void Awake()
        {
            if (Instance == null)
            {
                Instance = this;
            }
            else Destroy(this);
        }

        // Use this for initialization
        void Start()
        {
            State = new InputState();
        }

        // Update is called once per frame
        void Update()
        {

        }
    }
}