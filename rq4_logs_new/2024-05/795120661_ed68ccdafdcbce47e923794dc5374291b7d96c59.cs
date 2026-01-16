#nullable enable
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using VContainer;


namespace Daipan.LevelDesign.Player.Scripts
{
    public class PlayerParamsServer
    {
        readonly PlayerParams _playerParams = null;

        [Inject]
        PlayerParamsServer (PlayerParams playerParams)
        {
            _playerParams = playerParams;
        }


        public int GetHPAmount()
        {
            return _playerParams.HPAmount;
        }
        public PlayerAttackParameter GetAttackAmount()
        {
            return _playerParams.playerAtatckParameter;
        }
    }
}