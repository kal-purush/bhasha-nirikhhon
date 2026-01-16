using UnityEngine;
using System.Collections;

public class Tree : MonoBehaviour {
    public Ctrl _ctrl = null;
	public Animator _animator = null;
    public GameObject _nextTree = null;
    public float _treeLifeNum = 20;
    
    void FixedUpdate()
    {
        _treeLifeNum -= 0.02f;
        if(_treeLifeNum <= 0){
            _ctrl.fsmPost("end");
        }else if(_treeLifeNum >= 50 && _nextTree != null){
            _nextTree.SetActive(true);
            this.gameObject.SetActive(false);
            _treeLifeNum = 20;
        }
    }
    
    public void AwardStateOver(){
        _animator.SetBool("Clear", false);
    }
    
}