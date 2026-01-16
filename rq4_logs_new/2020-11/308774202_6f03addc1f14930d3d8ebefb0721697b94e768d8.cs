//-----------------------------------------------------------------------
// <copyright file="MainMenuUserInterfaceManager.cs" company="PERIGON GAMES">
//     Copyright (c) PERIGON GAMES. 2020 All Rights Reserved
// </copyright>
//-----------------------------------------------------------------------

using System;
using UnityEngine;

namespace PersonalDevelopment
{
    public class MainMenuUserInterfaceManager: MonoBehaviour
    {
        private readonly IStateManager _stateManager = new StateManager();

        [SerializeField] private StartScreenBehaviour _startScreen = null;
        [SerializeField] private PlayerJoinScreenBehaviour _playerJoinScreen = null;
        
        private void CleanUp()
        {
            _startScreen.CleanUp();
            _startScreen.OnButtonPressed -= OnStartButtonPressed;
            _stateManager.OnStateChanged -= OnStateChange;
        }

        private void OnStartButtonPressed()
        {
            _stateManager.SetState(State.CharacterSelection);
        }

        private void OnStateChange(State state)
        {
            bool isStartScreenActive = state == State.StartMenu;

            if (state == State.CharacterSelection)
            {
                isStartScreenActive = false;
            }
            
            _startScreen.gameObject.SetActive(isStartScreenActive);
            _playerJoinScreen.gameObject.SetActive(!isStartScreenActive);
        }
        
        #region Mono
        private void OnEnable()
        {
            _startScreen.Initialize();
            _startScreen.OnButtonPressed += OnStartButtonPressed;
            _stateManager.OnStateChanged += OnStateChange;
        }
        
        private void Start()
        {
            _stateManager.SetState(State.StartMenu);
        }
        
        private void OnDisable()
        {
            CleanUp();
        }

        private void OnDestroy()
        {
            CleanUp();
        }

        #endregion
    }
}