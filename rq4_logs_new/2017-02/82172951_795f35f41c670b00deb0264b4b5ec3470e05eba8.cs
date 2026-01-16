using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using CombatWorld.Map;
using CombatWorld.Units;
using CombatWorld.Utility;

namespace CombatWorld
{
	public class GameController : Singleton<GameController>
	{
		List<Node> allNodes = new List<Node>();
		Team currentTeam;

		public void AddNode(Node node)
		{
			allNodes.Add(node);
		}

		public void EndTurn()
		{
			switch (currentTeam)
			{
				case Team.Player:
					currentTeam = Team.AI;
					break;
				case Team.AI:
					currentTeam = Team.Player;
					break;
				default:
					break;
			}
			ResetAllNodes();
			SelectTeamNodes();
		}

		void ResetAllNodes()
		{
			foreach (Node node in allNodes)
			{
				node.ResetState();
			}
		}

		void SelectTeamNodes()
		{
			foreach (Node node in allNodes)
			{
				if (node.HasOccupant() && node.GetOccupant().GetTeam() == currentTeam)
				{
					node.SetState(HighlightState.Selectable);
				}
			}
		}

	}
}