using UnityEngine;
using UnityEditor;

public class LeaderboardEditor : MonoBehaviour {
    [MenuItem("Leaderboard/Clear")]
    static void ClearScore(){
        Leaderboard.Clear();
    }
}