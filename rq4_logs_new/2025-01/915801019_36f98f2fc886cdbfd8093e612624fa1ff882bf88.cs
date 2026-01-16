using Sample.Scripts.Views;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

namespace Sample.Scripts.Components
{
    public class ProgressBarComponent : BaseViewComponent
    {
        private int _currentValue;

        [SerializeField] private TextMeshProUGUI currentValueText;

        [SerializeField] private Slider slider;

        public int CurrentValue
        {
            get => _currentValue;
            set
            {
                _currentValue = value;
                slider.value = value;
                UpdateView();
            }
        }

        public float MaxValue
        {
            get => slider.maxValue;
            set
            {
                slider.maxValue = value;
                UpdateView();
            }
        }
        
        public float MinValue
        {
            get => slider.minValue;
            set => slider.minValue = value;
        }
        
        private void UpdateView()
        {
            currentValueText.text = $"{slider.value}/{MaxValue}";
        }
    }
}