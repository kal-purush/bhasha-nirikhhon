using System;
using _Scripts.PlayerScripts;
using UnityEngine;

namespace _Scripts
{
	public class Game : MonoBehaviour
	{
		[SerializeField] private PlayerManager _playerManager;
		
		private int _playersAwake = 0;
		
		private void Start()
		{
			EventBus<PlayerSleepEvent>.Subscribe(OnPlayerSleep);
		}	
		
		private void OnPlayerSleep(PlayerSleepEvent pEvent)
		{
			if (_playersAwake == 0) 
				_playersAwake = _playerManager.transform.childCount;

			_playersAwake--;
			if (_playersAwake == 1)
			{
				EndGame();
			}
			Debug.Log("Player is sleeping");
		}

		private void EndGame()
		{
			Debug.Log("Ending game");
			God.instance.SwapScene("FinishScene");
		}

		private void OnDisable()
		{
			EventBus<PlayerSleepEvent>.UnSubscribe(OnPlayerSleep);
		}
	}
}