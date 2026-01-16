using Assets.Scripts.SaveLoad;
using System.Collections.Generic;
using UnityEngine;

public class CollectTrial : Interactable, IDataPersist
{
    [SerializeField] string id;

    [SerializeField] private bool _completed;
    [SerializeField] private bool _challenging;
    [SerializeField] private GameObject _collectLists;
    [SerializeField] private List<GameObject> _collectList;
    private int _collectCount;

    public void LoadData(in GameData data)
    {

    }

    public void SaveData(ref GameData data)
    {

    }

    private void Awake()
    {
        _collectList = new List<GameObject>();
        foreach (Transform child in _collectLists.transform)
        {
            _collectList.Add(child.gameObject);
        }
        CancelTrial();
    }

    public override void Update()
    {
        base.Update();
        if (_challenging && _collectCount <= 0)
        {
            CancelTrial();
            _completed = true;
            //Reward
        }
    }

    public override void Interact()
    {
        if (!_completed)
        {
            if (!_challenging)
            {
                StartTrial();
            }
            else
            {
                CancelTrial();
            }
        }
    }

    public void StartTrial()
    {
        Debug.Log("Start Trial");
        _challenging = true;
        _collectCount = _collectList.Count;
        _collectList.ForEach(go => { go.SetActive(true); });
    }

    public void CancelTrial()
    {
        _challenging = false;
        _collectCount = _collectList.Count;
        _collectList.ForEach(go => { go.SetActive(false); });
    }

    public void Collect()
    {
        if (_challenging) _collectCount -= 1;
    }


}