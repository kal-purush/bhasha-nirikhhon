using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class NpcSpawner {
    private List<GameObject> npcPrefabs;
    private int numOfNpc;
    private Vector2 center;
    private Vector2 size;

    public List<Character> SpawnNpc(int i_numOfNpc, Vector2 i_center, Vector2 i_size) {
        numOfNpc = i_numOfNpc;
        center = i_center;
        size = i_size;
        List<Character> npcs = new List<Character>();

        for (int i = 0; i < numOfNpc; ++i) {
            int index = Random.Range(0, npcPrefabs.Count);
            Character npc = ((GameObject)Object.Instantiate(npcPrefabs[index], Vector2.zero, Quaternion.identity)).GetComponent<Character>();
            Vector2 position = new Vector2(Random.Range(0, size.x) - size.x/2, Random.Range(0, size.y) - size.y/2);
            position += center;
            npc.GetComponent<Transform>().position = position;
            npcs.Add(npc);
        }
        return npcs;
    }

    public void SetNpcPrefabs(List<GameObject> i_npcPrefabs) {
        npcPrefabs = i_npcPrefabs;
    }
}