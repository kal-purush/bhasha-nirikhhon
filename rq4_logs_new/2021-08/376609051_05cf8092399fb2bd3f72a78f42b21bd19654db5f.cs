using System;
using UnityEngine;

namespace Controller
{
    public class SaveController : MonoBehaviour
    {
        private PlayerMove _playerMove;
        private SaveDataRepository _saveDataRepository;

        private void Awake()
        
        {
            _playerMove = FindObjectOfType<PlayerMove>();
            _saveDataRepository = new SaveDataRepository();
        }

        private void Update()
        {
            if (Input.GetKeyDown(KeyCode.V))
            {
                _saveDataRepository.Save(_playerMove);
            }

            if (Input.GetKeyDown(KeyCode.B))
            { 
                _saveDataRepository.Load(_playerMove);
            }
        }
    }
}