using System.Collections.Generic;
using System.Linq;
using DebugPlus.Config;
using DebugPlus.Utils;
using EFT.Interactive;
using UnityEngine;

using OCB = DebugPlus.Utils.OverlayContentBuilder;

namespace DebugPlus.Components;

public class DoorDebug : MonoBehaviour
{
	private List<WorldInteractiveObject> _doors = [];

	private void Awake()
	{
		// This method is just lol, but works for this use case.
		_doors = LocationScene.GetAllObjectsAndWhenISayAllIActuallyMeanIt<WorldInteractiveObject>()
			.Where(o => o is Door)
			.ToList();

		foreach (var door in _doors)
		{
			door.GetOrAddComponent<OverlayProvider>()
				.SetOverlayContent(GetDoorInfoText(door), Enable);
		}
	}

	private static bool Enable()
	{
		return DebugPlusConfig.ShowDoorOverlays.Value;
	}
	
	private static string GetDoorInfoText(WorldInteractiveObject door)
	{
		OCB.Clear();
		
		OCB.AppendLabeledValue("DoorId", door.Id, Color.gray, Color.green);
		OCB.AppendLabeledValue("KeyId", door.KeyId ?? "No key", Color.gray, Color.green);
		OCB.AppendLabeledValue("Operable", door.Operatable.ToString(), Color.gray, Color.green);
		OCB.AppendLabeledValue("State", GetDoorState(door), Color.gray, Color.green);
		
		return OCB.ToString();
	}

	private static string GetDoorState(WorldInteractiveObject door)
	{
		return door.DoorState.ToString();
	}
}