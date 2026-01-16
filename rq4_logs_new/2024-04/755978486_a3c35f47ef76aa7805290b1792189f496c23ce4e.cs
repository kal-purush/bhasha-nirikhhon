using Assets.Scripts.Manager;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

namespace Assets.Scripts.Interfaces
{
    public class CurrencyUI : MonoBehaviour
    {

        [SerializeField] private Image _outerCircleImage;
        [SerializeField] private Image _imageBG;
        [SerializeField] private Color _bgNormalColor;
        [SerializeField] private Color _bgActiveColor;

        [SerializeField] private TMP_Text _bombCount;
        [SerializeField] private TMP_Text _currency;



        private void Awake()
        {
            ResourceManager.OnCurrenyChange += UpdateUI;
        }

        private void Start()
        {
            UpdateUI(0);
        }

        public void UpdateUI(int currency)
        {
            int bombCount = ResourceManager.Instance.GetBombCount();
            _bombCount.text = bombCount.ToString();
            if (bombCount > 0)
            {
                _imageBG.color = _bgActiveColor;
            }
            else
            {
                _imageBG.color = _bgNormalColor;
            }
            _currency.text = ResourceManager.Instance.GetCurrency().ToString();
            _outerCircleImage.fillClockwise = true;
            Debug.Log(ResourceManager.Instance.GetCurrencyPercentage());
            _outerCircleImage.fillAmount = ResourceManager.Instance.GetCurrencyPercentage();
        }

        private void OnDisable()
        {
            ResourceManager.OnCurrenyChange -= UpdateUI;
        }
    }
}