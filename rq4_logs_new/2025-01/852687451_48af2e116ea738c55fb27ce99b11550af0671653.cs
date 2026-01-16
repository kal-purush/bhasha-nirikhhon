using TMPro;
using UnityEngine;

public class LevelStatsCounterUI : MonoBehaviour
{
    [SerializeField] private TMP_Text _earnedMoneyText;
    [SerializeField] private TMP_Text _killedEnemiesText;

    private LevelStatsCounter _levelStatsCounter;
    private PlayerStats _playerStats;

    public void Initialize(LevelStatsCounter levelStatsCounter, PlayerStats playerStats)
    {
        _levelStatsCounter = levelStatsCounter;
        _playerStats = playerStats;
    }

    public void WriteStats()
    {
        _earnedMoneyText.text = _levelStatsCounter.EarnedMoney.ToString();
        _killedEnemiesText.text = _levelStatsCounter.KilledEnemies.ToString();
        _playerStats.Money += _levelStatsCounter.EarnedMoney;
    }
}