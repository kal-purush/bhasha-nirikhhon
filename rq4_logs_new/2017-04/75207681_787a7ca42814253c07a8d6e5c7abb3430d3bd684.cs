using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using UnityEngine;
using UnityEngine.Networking;

public class NW_Manager : NetworkManager {

    public override void OnServerConnect(NetworkConnection conn) {
        base.OnServerConnect(conn);

        print(MethodBase.GetCurrentMethod().Name);
        NW_Discovery.StopDiscovery();
    }

    public override void OnClientDisconnect(NetworkConnection conn) {
        base.OnClientDisconnect(conn);

        print(MethodBase.GetCurrentMethod().Name);
        RestartGame.RestartNow();
    }

    public override void OnServerDisconnect(NetworkConnection conn) {
        base.OnServerDisconnect(conn);

        print(MethodBase.GetCurrentMethod().Name);
        RestartGame.RestartNow();
    }
}