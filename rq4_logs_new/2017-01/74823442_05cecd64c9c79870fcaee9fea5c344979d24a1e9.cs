using System;
using UnityEngine;

namespace VillagerAction
{
	abstract public class Action
	{
		abstract public GameObject Subject {
			get;
			set;
		}

		abstract public bool Finished {
			get;
		}

		abstract public bool Finishable {
			get;
		}

		abstract public void Update();
	}
}
