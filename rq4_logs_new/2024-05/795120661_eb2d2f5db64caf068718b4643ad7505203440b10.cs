#nullable enable
using System;
using System.Collections;
using System.Collections.Generic;
using Daipan.Core.Interfaces;
using Unity.VisualScripting;
using UnityEngine;
using VContainer.Unity;

namespace Daipan.Utility.Scripts
{
    public class Timer : IUpdate
    {
        private float _currentTime = 0f;
        
        private bool _isPlaying = false;


        void IUpdate.Update()
        {
            if(!_isPlaying) return;
            _currentTime += Time.deltaTime;
        }
        public void Start()
        {
            _isPlaying = true;
            _currentTime = 0f;
        }
        public void Stop()
        {
            _isPlaying = false;
        }
        public void Resume()
        {
            _isPlaying = true;
        }
        public float GetCurrentTime()
        {
            return _currentTime;
        }
    }
}