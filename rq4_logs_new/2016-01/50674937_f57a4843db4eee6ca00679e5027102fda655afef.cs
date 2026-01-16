using UnityEngine;
using System.Collections;
using GDGeek;

public class Ctrl : MonoBehaviour {
    public View _view = null;
	public float _playerMoveSpeed = 0;
    public float _enemyMoveTime = 0;

    private Vector3 velocity_ = Vector2.zero;
    private bool isPlayState = false;
    private bool isHaveFollower = false;
    private FSM fsm_ = new FSM();

    void Start()
    {
        fsm_.addState("begin", beginState());
        fsm_.addState("play", playState());
        fsm_.addState("end", endState());
        fsm_.init("begin");
    }

    void Update () 
    {
        
    }

    void FixedUpdate()
    {
        if (isPlayState){ 
            PlayerMove();
            EnemyMove();
            if(isHaveFollower){
                FollowerMove();
            }
        }
    }
    
    
    public void fsmPost(string msg)
    {
        fsm_.post(msg);
    }
    
    private State beginState()
    {
        StateWithEventMap state = new StateWithEventMap();
        state.onStart += delegate{
            _view._beginPanel.SetActive(true);
            _view._playPanel.SetActive(false);
            _view._endPanel.SetActive(false);
        };
        state.onOver += delegate{
            _view._beginPanel.SetActive(false);
            _view._playPanel.SetActive(true);
        };
        state.addEvent("begin", "play");
        return state;
    }
    
    private State playState()
    {
        // StateWithEventMap state = TaskState.Create(delegate{
        //     return new Task();
        // }, fsm_, "end");
        StateWithEventMap state = new StateWithEventMap();
        state.onStart += delegate{
            isPlayState = true;
        };
        state.onOver += delegate{
            isPlayState = false;
        };
        return state;
    }
    
    private State endState()
    {
        StateWithEventMap state = new StateWithEventMap();
        state.onStart += delegate{
            
        };
        state.onOver += delegate{
            
        };
        return state;
    }
    
    private void PlayerMove()
    {
        velocity_ = new Vector3(Input.GetAxisRaw("Horizontal"), Input.GetAxisRaw("Vertical"), 0).normalized * _playerMoveSpeed;
        _view._player.transform.position = _view._player.transform.position + velocity_ * Time.fixedDeltaTime;
    }
    
    private void EnemyMove()
    {
        Move(_view._enemy.gameObject, _enemyMoveTime, _view._player.transform.position);
    }
    
    private void FollowerMove()
    {
        Vector3 pos = _view._player.transform.position + Vector3.one;
        Move(_view._follower.gameObject, _enemyMoveTime, pos);
    }
    
    private void Move(GameObject ob, float time, Vector3 position)
    {
        TweenLocalPosition comp = Tween.Begin<TweenLocalPosition>(ob, time);
        comp.from = comp.position;
        comp.to = position;

        if (_enemyMoveTime <= 0f)
        {
            comp.Sample(1f, true);
            comp.enabled = false;
        }
    }
    
    
    
    
}