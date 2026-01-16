using System.Collections;
using System.Collections.Generic;
using TeamB_TD.Battle.Unit.Enemy;
using UnityEngine;

public class GameResultController : MonoBehaviour
{
    [SerializeField] GameResultViewer _gameResultViewer;

    public void ResultScoreSet()
    {
        EnableResultPanel();
        //_gameResultViewer.DeadEnemyCountSet(EnemyCounter.Current.DeadEnemyCount);
        //_gameResultViewer.TowerHPSet();
    }

    public void EnableResultPanel()
    {
        this.gameObject.SetActive(true);
    }

    public void DisableResultPanel() 
    {
        this .gameObject.SetActive(false); 
    }
}