using System;
using UnityEngine;

namespace Controller
{
    public class InputController : PlayerMove
    {
        private readonly PlayerMove _player;
        private readonly SaveDataRepository _saveDataRepository;
        private readonly KeyCode _savePlayer = KeyCode.C;
        private readonly KeyCode _loadPlayer = KeyCode.V;

        public InputController(PlayerMove playerController)
        {
            _player = playerController;
            _saveDataRepository = new SaveDataRepository();
        }
       

        private void Update()
        {
            _player.Move();
            if (Input.GetKeyDown(_savePlayer))
            {
                _saveDataRepository.Save(_player);
                Debug.Log("x");
            }

            if (Input.GetKeyDown(_loadPlayer))
            { 
                _saveDataRepository.Load(_player);
            }
        }
    }
}