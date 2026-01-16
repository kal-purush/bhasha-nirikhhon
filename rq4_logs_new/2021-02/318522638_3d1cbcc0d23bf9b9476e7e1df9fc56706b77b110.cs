using Mirror;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AdvanceFloorZone : NetworkBehaviour
{
    private readonly List<Player> playersReady = new List<Player>();

    private ExtendedCoroutine changeLevelCoroutine;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.TryGetComponent(out Player player) == false)
            return;

        if (isServer)
        {
            playersReady.Add(player);

            if (playersReady.Count != PlayersDict.Instance.Players.Count)
                return;

            // all players are ready
            changeLevelCoroutine = new ExtendedCoroutine(this, CountdownToLevelChange(), GameManager.OnLoadNewLevel, true);
        }
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.TryGetComponent(out Player player) == false)
            return;

        if (isServer)
        {
            playersReady.Remove(player);
            if (changeLevelCoroutine != null)
            {
                changeLevelCoroutine.Stop(false);
                Debug.Log("Advancing abort!");
                changeLevelCoroutine = null;
            }
        }
    }

    private IEnumerator CountdownToLevelChange()
    {
        for (int i = 3; i >= 0; i--)
        {
            Debug.Log("Advancing in " + i + " seconds!");
            yield return new WaitForSeconds(1.0f);
        }
    }
}