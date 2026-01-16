#nullable enable
using System;
using Daipan.Viewer.Scripts;
using UnityEngine;
using VContainer;

namespace Daipan.Viewer.Tests
{
    public class PlayerTestInput : MonoBehaviour
    {
        DaipanExecutor _daipanExecutor;
        
        [Inject]
        public void Construct(DaipanExecutor daipanExecutor)
        {
            _daipanExecutor = daipanExecutor;
        }

        void Update()
        {
            if (Input.GetKeyDown(KeyCode.Space))
            {
                _daipanExecutor.DaiPan();
            } 
        }
    }
}