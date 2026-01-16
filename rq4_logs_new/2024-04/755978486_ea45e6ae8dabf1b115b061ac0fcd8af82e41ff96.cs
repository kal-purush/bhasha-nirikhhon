using Assets.Scripts.Combat;
using UnityEngine;

public class LaserShooter : MonoBehaviour
{
    [SerializeField] private bool _active = true;

    [SerializeField] private int _damagePerTick;
    [SerializeField] private float _delay;
    [SerializeField] private float _duration;
    private float _timer;

    [SerializeField] private Collider2D _collider;

    [SerializeField] private SpriteRenderer _spriteRenderer;
    [SerializeField] Color _normalColor;
    [SerializeField] Color _shootColor;


    private void Update()
    {
        if (_active)
        {
            _timer -= Time.deltaTime;
            if (_collider.gameObject.activeSelf)
            {
                if (_timer <= 0)
                {
                    _collider.gameObject.SetActive(false);
                    _timer = _delay;
                }
            }
            else
            {
                _spriteRenderer.color = Color.Lerp(_normalColor, _shootColor, (_delay - _timer) / _delay);
                if (_timer <= 0)
                {
                    _collider.gameObject.SetActive(true);
                    _timer = _duration;
                }
            }
        }
    }

    private void OnTriggerStay2D(Collider2D collision)
    {
        if (collision.CompareTag("Player") && collision.gameObject.TryGetComponent(out Health health))
        {
            health.TakeDamage(_damagePerTick, collision.transform.position - transform.position, 0.6f);
        }
    }
}