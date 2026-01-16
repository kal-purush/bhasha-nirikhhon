using System;
using UnityEngine;
using UnityEngine.UI;

namespace TeamB_TD
{
    namespace Battle
    {
        namespace Unit
        {
            namespace VFX
            {
                public class DamageVFX : MonoBehaviour
                {
                    [SerializeField]
                    private float _lifeTime;
                    [SerializeField]
                    private float _moveSpeed;

                    [SerializeField]
                    private RectTransform _rectTransform;
                    [SerializeField]
                    private Vector3 _offset;
                    [SerializeField]
                    private Text _text;

                    private float _timer = 0f;

                    public void Initialize(float damageValue)
                    {
                        _rectTransform.position += _offset;
                        _text.text = damageValue.ToString();
                    }

                    public void Update()
                    {
                        var gameSpeed = GameSpeedController.CurretGameSpeed;
                        _timer += Time.deltaTime * gameSpeed;
                        _rectTransform.Translate(new Vector3(0f, _moveSpeed * Time.deltaTime * gameSpeed, 0f));

                        if (_timer > _lifeTime)
                        {
                            Destroy(this.gameObject);
                        }
                    }
                }
            }
        }
    }
}