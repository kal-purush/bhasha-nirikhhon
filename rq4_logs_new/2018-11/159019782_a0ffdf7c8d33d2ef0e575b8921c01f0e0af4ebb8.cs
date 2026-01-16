using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace FPS
{
    public class FlashLightChargeView : MonoBehaviour
    {
        private Image _fillImage;
        private FlashLightModel _flashLightModel;

        private void Awake()
        {
            _fillImage = GetComponent<Image>();
            _flashLightModel = FindObjectOfType<FlashLightModel>();
            _flashLightModel.ChargeAmountChanged += OnChargeAmountChanged;
            OnChargeAmountChanged(_flashLightModel.ChargeAmount);
        }

        private void OnChargeAmountChanged(float amount) { _fillImage.fillAmount = amount; }

        private void OnDestroy()
        {
            _flashLightModel.ChargeAmountChanged -= OnChargeAmountChanged;
        }
    }
}