using System;
using System.Collections.Generic;
using Mirror;
using Sim.Interactables;
using UnityEngine;

public class BuildingBehavior : NetworkBehaviour {
    [Header("Settings")]
    [SerializeField]
    private string streetName;

    [SerializeField]
    private HallController hallPrefab;

    [SerializeField]
    private int hallSpacingY = 100;

    [SerializeField]
    private Teleporter mainElevator;

    private Dictionary<int, HallController> hallControllerByFloor = new Dictionary<int, HallController>();

    public override void OnStartServer() {
        Teleporter.OnUse += TeleportToFloor;
    }

    public override void OnStopServer() {
        Teleporter.OnUse -= TeleportToFloor;
    }

    [ServerCallback]
    public void TeleportToFloor(Teleporter elevator, int originFloor, int targetFloor, NetworkConnectionToClient conn) {
        if (targetFloor == 0) {
            // TELEPORT PLAYER TO MAIN HALL
            Debug.Log("Teleport to main hall");
            conn.Send(new TeleportMessage {destination = this.mainElevator.SpawnTransform.position});

            // Destroy hall if no players are in
            if (hallControllerByFloor.ContainsKey(originFloor)) {
                hallControllerByFloor[originFloor].RemovePlayer(conn.identity);
                
                if (!hallControllerByFloor[originFloor].ContainPlayers()) {
                    NetworkServer.Destroy(hallControllerByFloor[originFloor].gameObject);
                    hallControllerByFloor.Remove(originFloor);
                } else {
                    Debug.Log($"Can't destroy hall with floor number {originFloor} because not empty");
                }
            } else {
                Debug.LogError($"Can't find hall with floor number {originFloor}");
            }
        } else {
            HallController hallController = null;

            if (hallControllerByFloor.ContainsKey(targetFloor)) {
                hallController = hallControllerByFloor[targetFloor];
            } else {
                // CREATE FLOOR
                HallController newHallController = Instantiate(this.hallPrefab, new Vector3(0, -this.hallSpacingY * targetFloor, 0), Quaternion.identity);

                NetworkServer.Spawn(newHallController.gameObject);

                newHallController.Init(streetName, targetFloor);

                hallControllerByFloor.Add(targetFloor, newHallController);

                hallController = newHallController;
            }

            // TELEPORT PLAYER
            hallController.MoveToSpawn(conn);
        }
    }
}