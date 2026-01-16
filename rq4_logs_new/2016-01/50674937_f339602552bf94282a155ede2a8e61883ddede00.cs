using UnityEngine;
using System.Collections;
using GDGeek;

public class Ctrl : MonoBehaviour {

    public GameObject _player = null;
    public GameObject _Enemy = null;
	public float _playerMoveSpeed = 0;
    public float _enemyMoveTime = 0;

    private Vector3 velocity_ = Vector2.zero;

    void Start()
    {
        
    }

    void Update () 
    {
        EnemyMove();
    }

    void FixedUpdate()
    {
        Move();
    }
    
    private void Move()
    {
        velocity_ = new Vector3(Input.GetAxisRaw("Horizontal"), Input.GetAxisRaw("Vertical"), 0).normalized * _playerMoveSpeed;
        _player.transform.position = _player.transform.position + velocity_ * Time.fixedDeltaTime;
    }
    
    private void EnemyMove()
    {
        TweenLocalPosition comp = Tween.Begin<TweenLocalPosition>(_Enemy.gameObject, _enemyMoveTime);
        comp.from = comp.position;
        comp.to = _player.transform.position;

        if (_enemyMoveTime <= 0f)
        {
            comp.Sample(1f, true);
            comp.enabled = false;
        }
        
    }
    
    
    
}