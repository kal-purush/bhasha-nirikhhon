using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using VContainer;
using VContainer.Unity;

using VibrationUtility;



public class VibrationService : IInitializable, IDisposable
{
    [Inject] IObjectResolver _container;

    GameObject _vibeObject;

    IDisposable _disposable;


    public void Initialize()
    {
        if (_vibeObject == null)
        {
            _vibeObject = new GameObject() { name = "VibrationService" };
            UnityEngine.Object.DontDestroyOnLoad(_vibeObject);
        }
        else
        {
            Debug.LogError($"Vibration Service Make Twice!! Please Check");
        }

        VibrationUtil.Init();
    }

    public void Dispose()
    {
        _disposable?.Dispose();
        _disposable = null;
    }
}