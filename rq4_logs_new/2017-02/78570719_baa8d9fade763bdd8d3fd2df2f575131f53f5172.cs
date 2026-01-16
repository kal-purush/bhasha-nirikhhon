using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TAMKShooter.Systems;

namespace TAMKShooter.Level
{
	public abstract class ConditionBase : MonoBehaviour
	{
		public LevelManager LevelManager { get; private set; }
		public bool IsConditionMet { get; protected set; }

		public void Init(LevelManager levelManager)
		{
			LevelManager = levelManager;
			Initialize ();
		}

		protected abstract void Initialize ();
	}
}