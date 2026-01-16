using UnityEngine;

namespace KamranWali.CodeOptPro.Bars
{
    public abstract class BaseBar : MonoBehaviour, IBar
    {
        protected int maxValue;
        protected int curValue;

        public virtual void StartSetup(int maxValue)
        {
            this.maxValue = maxValue;
            curValue = maxValue;
        }

        public virtual float GetNormalValue() => ((float)curValue) / ((float)maxValue);
        public virtual void AddValue(int value) => curValue = curValue + value >= maxValue ? maxValue : curValue + value;
        public virtual int GetCurrentValue() => curValue;
        public void SetCurrent(int value) => curValue = value >= maxValue ? maxValue : value >= 0 ? value : 0;
        public virtual int GetMaxValue() => maxValue;
        public virtual bool IsDepleted() => curValue == 0;
        public virtual void Restore() => curValue = maxValue;
        public abstract void RemoveValue(int value);
    }
}