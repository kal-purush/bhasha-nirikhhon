using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class UnitAttack : MonoBehaviour
{
    [SerializeField]
    private UnitModelView _model;
    Transform _modelTransform = null;
    Transform _transform = null;

    float _speed = 1f;
    float _rotationSpeed = 5f;

    BaseUnitBehaviour _target = null;
    Transform _targetTransform = null;

    Vector3 _destinationPosition = Vector3.zero;

    Action<BaseUnitBehaviour> _onTargetReached = null;

    Dictionary<EUnitAttackState, Action> _attackActions = new Dictionary<EUnitAttackState, Action>();

    EUnitAttackState _state = EUnitAttackState.None;
    public EUnitAttackState State
    {
        get { return _state; }
        private set
        {
            _state = value;
            Update();
        }
    }

    public void Awake()
    {
		if (_model == null) 
			_model = gameObject.GetComponentInChildren<UnitModelView>();
        _model.MovementSpeed = _speed;
        _modelTransform = _model.transform;
        _transform = transform;

        _attackActions.Add(EUnitAttackState.WatchTarget, WatchTarget);
        _attackActions.Add(EUnitAttackState.NoAttack, MoveToPosition); 
        _attackActions.Add(EUnitAttackState.LookIntoSunset, LookForward);
        _attackActions.Add(EUnitAttackState.WalkIntoSunset, MoveForward);
    }

    public void Update()
    {
        if (_state != EUnitAttackState.None && _state != EUnitAttackState.NoTarget)
        {
            if (_attackActions.ContainsKey(_state))
                _attackActions[_state]();
        }
    }

    #region Movement

    public void LookIntoSunset()
    {
        State = EUnitAttackState.LookIntoSunset;
    }

    public void WalkIntoSunset()
    {
        State = EUnitAttackState.WalkIntoSunset;
        _model.PlayRunAnimation();
    }

    public void TargetAttack(BaseUnitBehaviour unit, ArrayRO<BaseUnitBehaviour> possibleTargets,
        Action<BaseUnitBehaviour> onTargetFound, Action<BaseUnitBehaviour> onTargetReached)
    {
        Reset(false);
        UnitSet.Instance.SetUnitPositions();

        _onTargetReached = onTargetReached;
        _target = GetTarget(unit, possibleTargets);
        if (_target != null)
        {
            _targetTransform = _target.transform;
        }
        if (_targetTransform == null)
        {
            Reset(false);
            State = EUnitAttackState.NoTarget;
            return;
        }
        onTargetFound(_targetTransform.gameObject.GetComponent<BaseUnitBehaviour>());
        if (State == EUnitAttackState.None || State == EUnitAttackState.WatchTarget)
        {
            StartTargetAttack(_target);
        }
    }

    BaseUnitBehaviour GetTarget(BaseUnitBehaviour unit, ArrayRO<BaseUnitBehaviour> possibleTargets)
    {
        List<BaseUnitBehaviour>[] rangeUnits = UnitSet.Instance.GetRangeUnits(possibleTargets);
        List<BaseUnitBehaviour> targetUnits = unit.UnitData.Data.BasePriority == EUnitRange.Melee ? rangeUnits[UnitSet.ThirdZoneIndex]
            : (unit.UnitData.Data.BasePriority == EUnitRange.Ranged ? rangeUnits[UnitSet.SecondZoneIndex] : null);

        if (targetUnits == null || targetUnits.Count == 0)
        {
            targetUnits = unit.UnitData.Data.BasePriority == EUnitRange.Melee ? rangeUnits[UnitSet.SecondZoneIndex]
                : (unit.UnitData.Data.BasePriority == EUnitRange.Ranged ? rangeUnits[UnitSet.ThirdZoneIndex] : null);
            if (targetUnits == null || targetUnits.Count == 0)
                targetUnits = rangeUnits[UnitSet.FirstZoneIndex];
        }
        if (targetUnits == null || targetUnits.Count == 0)
            return null;
        targetUnits.Sort();
        return targetUnits[0]; 
    }

    void StartTargetAttack(BaseUnitBehaviour target)
    {
        //_model.StopCurrentAnimation();
        Action<BaseUnitBehaviour> onTargetReached = _onTargetReached;
        _onTargetReached = null;
        StopAllCoroutines();

        State = EUnitAttackState.WatchTarget;
        onTargetReached(target);
    }

    private void WatchTarget()
    {
        _modelTransform.localRotation = Quaternion.Lerp(_modelTransform.localRotation, 
            Quaternion.LookRotation(_targetTransform.transform.position - _transform.position), _rotationSpeed * Time.deltaTime);
        //_cachedModelTransform.LookAt(_nearestTarget.transform);
    }

    private void LookForward()
    {
        _modelTransform.localRotation = Quaternion.Lerp(_modelTransform.localRotation, 
            Quaternion.LookRotation(new Vector3(1f, 0f, 0f)), _rotationSpeed * Time.deltaTime);
    }

    private void MoveForward()
    {
        Vector3 destination = _modelTransform.position + new Vector3(1f, 0f, 0f);
        _modelTransform.localRotation = Quaternion.Lerp(_modelTransform.localRotation,
            Quaternion.LookRotation(new Vector3(1f, 0f, 0f)), _rotationSpeed * Time.deltaTime);
        //_modelTransform.LookAt(destination);
        _transform.position = Vector3.MoveTowards(_transform.position, destination, Time.deltaTime * _speed);
    }

    public void SetPosition(BaseUnitBehaviour unit, Vector3 position)
    {
        if (_destinationPosition == Vector3.zero)
            _destinationPosition = unit.transform.position = position;
        else
        {
            _destinationPosition = position;
            if (_destinationPosition != _modelTransform.position)
            {
                State = EUnitAttackState.NoAttack;
                //_model.PlayRunAnimation();
            }
        }
    }

    public void MoveToPosition()
    {
        float minDistance = 0.1f;
        _transform.position = Vector3.MoveTowards(_modelTransform.position, _destinationPosition, Time.deltaTime * _speed * 3);
        if (Vector3.Distance(_transform.position, _destinationPosition) < minDistance)
        {
            //_model.StopCurrentAnimation();
            _destinationPosition = _transform.position;
            State = EUnitAttackState.None;
        }
    }

    #endregion

    public void Reset(bool full)
    {
        _target = null;
        _targetTransform = null;

        _onTargetReached = null;
		StopAllCoroutines();
        if (full)
            State = EUnitAttackState.None;
    }
}
