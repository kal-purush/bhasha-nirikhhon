using UniRx;
using UnityEngine;

namespace SimpleExample.Scripts.MVVM.DataSources
{
    [CreateAssetMenu(fileName = "InputsDatasource", menuName = "SimpleExample/DataSources/Inputs")]
    public class InputsDatasource : ScriptableObject
    {
        public readonly ReactiveProperty<(int, int)> Current = new ReactiveProperty<(int, int)>();

        public void SetFirst(string input) => Current.Value = (int.Parse(input), Current.Value.Item2);
        public void SetSecond(string input) => Current.Value = (Current.Value.Item1, int.Parse(input));

        public int GetFirstValue() => Current.Value.Item1;

        public int GetSecondValue() => Current.Value.Item2;
    }
}