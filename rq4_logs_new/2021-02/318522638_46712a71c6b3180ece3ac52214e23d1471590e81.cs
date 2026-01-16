using UnityEngine;

[CreateAssetMenu(menuName = "Gamemode/Gamemode")]
public class GameMode : ScriptableObject
{
    [HideInInspector] public int[] levelSeeds;

    public int levelAmount;
    //public GameRule[] gameRules;
    public int startingDifficulty;
    public Pickable[] startingItems;
    public int minPlayers, maxPlayers;
    // public DungeonConfig dungeonConfig;

    public void Init(int seed)
    {
        Random.InitState(seed);
        levelSeeds = new int[levelAmount];
        for (int i = 0; i < levelAmount; i++)
            levelSeeds[i] = Random.Range(int.MinValue, int.MaxValue);
    }
}