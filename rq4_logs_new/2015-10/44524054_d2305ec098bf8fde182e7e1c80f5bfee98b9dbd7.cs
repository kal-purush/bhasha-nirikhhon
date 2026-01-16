using UnityEngine;
using System.Collections;

public class EnemyShot : MonoBehaviour
{
    GameObject _player = null;

    Rigidbody _enemyShot = null;

    const string _playerName = "Player";

    public bool IsHitPlayer { get; private set; }

    Vector3 _moveVec;

    [SerializeField, Range(0.0f, 1.0f), Tooltip("速度の倍率")]
    float _speed = 1.0f;

    //--------------------------------------------

    void Awake()
    {
        _player = GameObject.Find(_playerName);
        IsHitPlayer = false;
    }

    void Start()
    {
        _moveVec =
            gameObject.transform.localPosition -
            _player.transform.localPosition;

        _enemyShot = gameObject.AddComponent<Rigidbody>();
        _enemyShot.useGravity = false;
    }

    void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.name == _playerName)
        {
            IsHitPlayer = true;
        }
    }

    void FixedUpdate()
    {
        _enemyShot.AddForce((_moveVec * 0.1f) * _speed, ForceMode.VelocityChange);
    }

    void OnBecameVisible()
    {
        Destroy(gameObject);
    }
}