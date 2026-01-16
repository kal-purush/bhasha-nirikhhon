using System.Collections.Generic;
using UnityEngine;

namespace TeamB_TD
{
    namespace Battle
    {
        namespace Unit
        {
            namespace Ally
            {
                public class PlacedAllyContainer : MonoBehaviour
                {
                    private static PlacedAllyContainer _current;
                    public static PlacedAllyContainer Current => _current;

                    [SerializeField]
                    private AllyUnitPlacer _allyPlacer;

                    private List<AllyController> _placedAllies = new List<AllyController>();

                    public IReadOnlyList<AllyController> PlacedAllies => _placedAllies; // 配置済みの全ての味方ユニット

                    private void Awake()
                    {
                        _current = this;

                        if (_allyPlacer) _allyPlacer.OnPlacedAlly += OnPlacedAlly;
                    }

                    public void OnPlacedAlly(AllyController allyController)
                    {
                        _placedAllies.Add(allyController);
                        allyController.OnDeadAlly += OnDeadAlly;
                    }

                    private void OnDeadAlly(AllyController allyController)
                    {
                        allyController.OnDeadAlly -= OnDeadAlly;
                        _placedAllies.Remove(allyController);
                    }
                }
            }
        }
    }
}