using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LevelSetManager : MonoBehaviour
{
    static private LevelSetManager instance = null;
    static private LevelSetManager setInstance() { instance = GameManager.Instance.GetComponentInChildren<LevelSetManager>(true); return instance; }
    static public LevelSetManager Instance { get { return instance == null ? setInstance() : instance; } }

    public List<LevelProps> allLevels;
    static public List<LevelProps> AllLevels { get => Instance.allLevels; }

    [SerializeField]
    private List<LevelProps> levels;

    void Start() {
        levels = new List<LevelProps>();
    }
}