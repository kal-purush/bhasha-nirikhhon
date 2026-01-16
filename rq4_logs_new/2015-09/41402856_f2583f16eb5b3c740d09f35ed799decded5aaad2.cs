using UnityEngine;
using System.Collections;

public static class GalagaHelper {

    public enum EntranceFlightPatterns
    {
        round1_DownLeft,
        round1_DownRight,
        rd1_LeftUp,
        rd1_RightUp,
        rd1_Left,
        rd1_Right
    }

    public static Transform[] EntrancePatterns(EntranceFlightPatterns pattern)
    {
        EnemySpawner spawner = GameObject.Find("EnemyFormation1").GetComponent<EnemySpawner>();
        GameObject spawnPt = GameObject.FindGameObjectWithTag("Respawn");
        GameObject middlePt = GameObject.FindGameObjectWithTag("Point2");
        GameObject rightEntrancePt = GameObject.FindGameObjectWithTag("begin_Right");
        GameObject leftEntrancePt = GameObject.FindGameObjectWithTag("begin_Left");
        Transform nextSpawnPos = spawner.currentSpawnPos;
        switch (pattern)
        {
            case EntranceFlightPatterns.round1_DownLeft:
                Transform[] rd1_Flight1 = new Transform[5];
                rd1_Flight1[0] = spawnPt.transform;
                rd1_Flight1[1] = middlePt.transform;
                rd1_Flight1[2] = leftEntrancePt.transform;
                rd1_Flight1[3] = spawner.gameObject.transform;
                rd1_Flight1[4] = nextSpawnPos;
                return rd1_Flight1;
                break;
            case EntranceFlightPatterns.round1_DownRight:
                Transform[] rd1_Flight2 = new Transform[5];
                rd1_Flight2[0] = spawnPt.transform;
                rd1_Flight2[1] = middlePt.transform;
                rd1_Flight2[2] = rightEntrancePt.transform;
                rd1_Flight2[3] = spawner.gameObject.transform;
                rd1_Flight2[4] = nextSpawnPos;
                return rd1_Flight2;
                break;
            default:
                Transform[] test = new Transform[2];
                test[0] = spawnPt.transform;
                test[1] = middlePt.transform;
                return test;
                break;
        }
    }

}