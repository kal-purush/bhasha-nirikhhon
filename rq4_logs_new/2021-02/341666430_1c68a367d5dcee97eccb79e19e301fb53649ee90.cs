using System;
using System.Collections;
using System.Collections.Generic;
using Unity.Mathematics;
using UnityEditor.U2D.Sprites;
using UnityEngine;
using UnityEngine.SocialPlatforms;

public class EndlessBackground : MonoBehaviour
{
    private Transform _player;
    private GameObject _mainBackground;
    private Transform[] _backgroundLayers;

    private Transform _ground;
    private Transform _sky;

    private void Start()
    {
        _ground = GameObject.Find("Ground").transform;
        _sky = GameObject.Find("BG0_World").transform;
        _player = GameObject.FindWithTag("Player").transform;
        _mainBackground = GameObject.Find("Background");
        int childCount = _mainBackground.transform.childCount;
        
        _backgroundLayers = new Transform[childCount - 1];
        
        for (int i = 1; i < childCount; i++)
        {
            _backgroundLayers[i - 1] = _mainBackground.transform.GetChild(i);
        }
    }

    private void Update()
    {
        float playerPositionX = GetTargetXPosition(_player);
        foreach (Transform backgroundLayer in _backgroundLayers)
        {
            float backgroundPositionX = backgroundLayer.GetChild(2).position.x;
            if (playerPositionX < backgroundPositionX + .1f && playerPositionX > backgroundPositionX - .1f)
            {
                MoveLayerToRight(backgroundLayer);
                MoveGroundAndSky();
            }
        }
    }

    private float GetTargetXPosition(Transform target)
    {
        return target.position.x;
    }

    private void MoveLayerToRight(Transform layer)
    {
        Vector3 firstLayerPosition = layer.GetChild(0).transform.position;
        int childCount = layer.childCount;
        layer.GetChild(0).transform.position =
            new Vector3(firstLayerPosition.x + (10.24f * childCount), firstLayerPosition.y, firstLayerPosition.z);
        layer.GetChild(0).SetSiblingIndex(childCount);
    }

    private void MoveGroundAndSky()
    {
        _ground.position = new Vector3(transform.position.x, _ground.position.y, _ground.position.z);
        _sky.position = new Vector3(transform.position.x, _sky.position.y, _sky.position.z);
    }
}