using System;
using System.Collections;
using System.Collections.Generic;
using Common;
using Scenes.GameOver;
using Scenes.Main;
using UnityEngine;

public class SwitchSceneOnDeath : MonoBehaviour
{
    [SerializeField] private EnemyHealthSetter _bossHealthSetter;
    [SerializeField] private float _sceneSwitchTime;

    private bool _sceneSwitchActive;

    private void Start()
    {
        _bossHealthSetter.OnHealthZero += HandleBossDead;
        
    }

    private void Update()
    {
        if (!_sceneSwitchActive)
        {
            return;
        }

        _sceneSwitchTime -= Time.deltaTime;
        if (_sceneSwitchTime <= 0)
        {
            GameOverSceneData.didPlayerWin = true;
            MainSceneController.Instance.FadeAndSwitchScene();
            _sceneSwitchActive = false;
        }
    }

    private void HandleBossDead()
    {
        _sceneSwitchActive = true;
    }
}