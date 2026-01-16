using Assets.Scripts.SaveLoad;
using System;
using System.Collections.Generic;
using UnityEngine;



public class FightArenaManager : MonoBehaviour, IDataPersist
{
    [Serializable]
    public class EnemiesList
    {
        public List<GameObject> enemies;
    }

    [SerializeField] private bool _completed;
    [SerializeField] private bool _challenging;

    [SerializeField] private int _waveIndex;
    public List<EnemiesList> enemiesWave = new List<EnemiesList>();

    [SerializeField] private GameObject[] _doors;

    [SerializeField] string id;
    void OnValidate()
    {
        if (string.IsNullOrEmpty(id))
        {
            id = Guid.NewGuid().ToString();
        }
    }

    private void Start()
    {
        foreach (GameObject door in _doors)
        {
            door.SetActive(false);
        }
        _challenging = false;
        _waveIndex = 0;
    }

    private void Update()
    {
        if (_challenging)
        {
            enemiesWave[_waveIndex].enemies.RemoveAll(item => item == null);
            if (enemiesWave[_waveIndex].enemies.Count == 0)
            {
                _waveIndex++;
                enemiesWave[_waveIndex].enemies.ForEach(enemy => { enemy.SetActive(true); });
            }
            if (_waveIndex > enemiesWave.Count - 1)
            {
                EndFight();
            }
        }

    }

    public void SaveData(ref GameData data)
    {
        data.fightArenas[id] = _completed;
    }

    public void LoadData(in GameData data)
    {
        if (data.fightArenas.TryGetValue(id, out bool val))
        {
            _completed = val;
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision != null && collision.CompareTag("Player") && !_completed)
        {
            StartFight();
        }
    }

    public void StartFight()
    {
        _waveIndex = 0;
        enemiesWave[_waveIndex].enemies.ForEach(enemy => { enemy.SetActive(true); });
        foreach (GameObject door in _doors)
        {
            door.SetActive(true);
        }
        _challenging = true;
        Debug.Log("Start Fight");
    }

    public void EndFight()
    {
        _challenging = false;
        foreach (GameObject door in _doors)
        {
            door.SetActive(false);
        }
    }
}