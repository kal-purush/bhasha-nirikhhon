using System.Collections.Generic;
using UnityEngine;

// 日本語対応
namespace TeamB_TD
{
    namespace Battle
    {
        namespace Unit
        {
            namespace Ally
            {
                public class AllyPlaceableManager : MonoBehaviour
                {
                    public static AllyPlaceableManager Instance { get; private set; } = null;
                    public Dictionary<int, (float Erapse, PlaceableStatus Placeable)> WaitForReviving => _waitForReviving;

                    [SerializeField]
                    private AllyPrefabContainer _allyPrefabs = null;

                    /// <summary>ユニットが亡くなった時、再設置可能になるまでの時間を計測する</summary>
                    private Dictionary<int, (float Erapse, PlaceableStatus Placeable)> _waitForReviving
                        = new Dictionary<int, (float, PlaceableStatus)>();
                    private Dictionary<int, float> _allysRevivalInterval = new Dictionary<int, float>();
                    private Queue<int> _waitingQueue = new Queue<int>();

                    private void Awake()
                    {
                        Instance = this;
                    }

                    private void Start()
                    {
                        if (_allyPrefabs == null) Debug.LogWarning("AllyPrefabContainer is not assigned");
                        else
                        {
                            foreach (var ally in _allyPrefabs.AllyPrefabs)
                            {
                                if (_waitForReviving.ContainsKey(ally.ConstantParams.ID)) continue;
                                _waitForReviving.Add(ally.ConstantParams.ID, (0.0f, PlaceableStatus.Placeable));
                                _allysRevivalInterval.Add(ally.ConstantParams.ID, ally.ConstantParams.RevivalInterval);
                            }
                        }
                    }

                    public bool IsAllyPlaceable(AllyController ally)
                    {
                        return _waitForReviving[ally.ConstantParams.ID].Placeable == PlaceableStatus.Placeable;
                    }

                    public void PlaceAlly(int allyId)
                    {
                        ChangeStatus(allyId, PlaceableStatus.HasPlaced);
                    }

                    public void OnDeadAlly(AllyController ally)
                    {
                        int allyId = ally.ConstantParams.ID;
                        ChangeStatus(allyId, PlaceableStatus.Reviving);
                        _waitingQueue.Enqueue(allyId);
                        ally.OnDeadAlly -= OnDeadAlly;
                    }

                    public void CalcRevivingTime()
                    {
                        if (_waitingQueue.Count == 0) return;

                        for (int i = 0, loopCount = _waitingQueue.Count; i < loopCount; i++)
                        {
                            int n = _waitingQueue.Dequeue();
                            _waitForReviving[n] =
                                (_waitForReviving[n].Erapse + Time.deltaTime * GameSpeedController.CurretGameSpeed,
                                PlaceableStatus.Reviving);

                            if (_waitForReviving[n].Erapse < _allysRevivalInterval[n]) { _waitingQueue.Enqueue(n); continue; }

                            ChangeStatus(n, PlaceableStatus.Placeable);
                        }
                    }

                    public void ChangeStatus(int allyId, PlaceableStatus status)
                    {
                        _waitForReviving[allyId] = (0.0f, status);
                    }
                }
            }
        }
    }
}