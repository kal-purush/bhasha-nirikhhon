using UnityEngine;
using UnityEngine.UI;

namespace TeamB_TD
{
    namespace Battle
    {
        namespace Unit
        {
            namespace Ally
            {
                public class AllyLifeViewer : MonoBehaviour
                {
                    [SerializeField]
                    private Slider _slider;
                    [SerializeField]
                    private AllyController _allyController;

                    public AllyLifeController LifeController => _allyController.LifeController;

                    private void Start()
                    {
                        if (_allyController == null)
                        {
                            Debug.LogWarning("AllyController is not assigned.");
                        }

                        _slider.minValue = 0f;
                        _slider.maxValue = LifeController.MaxLife;
                        _slider.value = LifeController.CurrentLife;
                    }

                    private void OnEnable()
                    {
                        LifeController.OnLifeChanged += ApplyValue;
                    }
                    private void OnDisable()
                    {
                        LifeController.OnLifeChanged -= ApplyValue;
                    }

                    private void ApplyValue(float value)
                    {
                        _slider.value = value;
                    }
                }
            }
        }
    }
}