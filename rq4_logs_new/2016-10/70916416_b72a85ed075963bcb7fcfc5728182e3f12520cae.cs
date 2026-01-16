using UnityEngine;
using RAIN.Action;
using RAIN.Core;

/// <summary>
/// Alerts all guards about the player's presence.
/// </summary>
[RAINAction]
public class AlertGuards : RAINAction {

    /// <summary>
    /// Updates the AI every frame.
    /// </summary>
    /// <param name="ai">The AI to update.</param>
    public override ActionResult Execute(AI ai) {
        ai.Body.GetComponent<AudioSource>().Play();
        GameObject player = (GameObject)ai.WorkingMemory.GetItem(SquadManager.PLAYER);
        SquadManager.instance.AlertAllGuards(player);
        return ActionResult.SUCCESS;
    }
}