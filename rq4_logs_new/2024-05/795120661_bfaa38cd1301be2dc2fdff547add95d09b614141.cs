#nullable enable
using UnityEngine;

namespace Stream.Viewer.Scripts
{
    public class IrritatedValue
    {
        public int Value => Mathf.Max(IncreasedValue - DecreasedValue, 0);
        int DecreasedValue { get; set; }
        int IncreasedValue { get; set; }

        public void IncreaseValue(int amount)
        {
            // [Prerequisite]
            if (amount < 0) Debug.LogWarning($"IrritatedValue.IncreaseValue() amount is negative : {amount}");
            
            IncreasedValue += amount;
        }

        public void DecreaseValue(int amount)
        {
            // [Prerequisite]
            if (amount < 0) Debug.LogWarning($"IrritatedValue.DecreaseValue() amount is negative : {amount}");

            if (Value <= 0) return;
            if (Value - amount < 0)
            {
                DecreasedValue += Value;
            }
            else
            {
                DecreasedValue += amount;
            }
        }
    }
}