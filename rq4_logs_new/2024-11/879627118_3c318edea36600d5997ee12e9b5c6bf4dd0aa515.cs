using UnityEngine;
using Pathfinding;

namespace Pathfinding
{
	[UniqueComponent(tag = "ai.destination")]
	[HelpURL("http://arongranberg.com/astar/docs/class_pathfinding_1_1_a_i_destination_setter.php")]
	public class AIDestinationSetter : VersionedMonoBehaviour
	{
		public Transform target;   // The target object this AI should follow

		private IAstarAI ai;
		private bool targetInTriggerZone = false;  // Flag to track if the target is in this enemy's TriggerZone
		public GameObject triggerZone;
		void OnEnable()
		{
			ai = GetComponent<IAstarAI>();
			if (ai != null) ai.onSearchPath += Update;
		}

		void OnDisable()
		{
			if (ai != null) ai.onSearchPath -= Update;
		}

		void Update()
		{
			// Only set the destination if the target is in the trigger zone
			if (target != null && ai != null && targetInTriggerZone)
			{
				ai.destination = target.position;
			}
		}

		// Trigger event when the target enters the TriggerZone
		private void OnTriggerEnter2D(Collider2D other)
		{
			// Check if the entering object is this enemy's target
			if (other.transform == target)
			{
				targetInTriggerZone = true;
			}
			print("target entered");
		}

		// Trigger event when the target exits the TriggerZone
		private void OnTriggerExit2D(Collider2D other)
		{
			// Check if the exiting object is this enemy's target
			if (other.transform == target)
			{
				targetInTriggerZone = false;
			}
			print("target left");
		}
	}
}