using Assets.Scripts.Model;
using Assets.Scripts.View;
using System;
using UnityEngine;

namespace Assets.Scripts.Controllers
{
	class GameController : MonoBehaviour, IDisposable
	{
		private ListExecuteObject _interactiveObject;
		private DisplayEndGame _displayEndGame;
		private DisplayBonuses _displayBonuses;
		private int _countBonuses;

		private void Awake()
		{
			_interactiveObject = new ListExecuteObject();
			_displayEndGame = new DisplayEndGame();
			_displayBonuses = new DisplayBonuses();
			foreach(var o in _interactiveObject)
			{
				if(o is BadBonus badBonus)
				{
					badBonus.OnCaughtPlayerChange += CaughtPlayer;
					badBonus.OnCaughtPlayerChange += _displayEndGame.GameOver;
				}

				if(o is GoodBonus goodBonus)
				{
					goodBonus.onPointChange += AddBonus;
				}
			}
		}

		private void CaughtPlayer(string value, Color Args)
		{
			Time.timeScale = 0.0f;
		}

		private void AddBonus(int value)
		{
			_countBonuses += value;
			_displayBonuses.Display(_countBonuses);
		}

		private void Update()
		{
			for(var i = 0; i < _interactiveObject.Length; i++)
			{
				var interactiveObject = _interactiveObject[i];
				if(interactiveObject == null)
				{
					continue;
				}
				interactiveObject.Execute();
			}
		}

		public void Dispose()
		{
			foreach(var o in _interactiveObject)
			{
				if(o is BadBonus badBonus)
				{
					badBonus.OnCaughtPlayerChange -= CaughtPlayer;
					badBonus.OnCaughtPlayerChange -= _displayEndGame.GameOver;
				}

				if(o is GoodBonus goodBonus)
				{
					goodBonus.onPointChange -= AddBonus;
				}
			}
		}
	}
}