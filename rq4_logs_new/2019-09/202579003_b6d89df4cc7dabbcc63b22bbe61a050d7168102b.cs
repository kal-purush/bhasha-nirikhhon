using Common;
using UnityEngine;

public class EnemyDeathController : MonoBehaviour
{
    [SerializeField] private EnemyHealthSetter _enemyHealthSetter;
    [SerializeField] private GameObject _enemyDeathEffect;
    [SerializeField] private Transform _effectSpawnPosition;
    [SerializeField] private GameObject _enemyBody;

    #region Unity Functions

    private void Start() => _enemyHealthSetter.OnHealthZero += HandleEnemyHealthZero;

    #endregion

    #region Utility Functions

    private void HandleEnemyHealthZero()
    {
        _enemyHealthSetter.OnHealthZero -= HandleEnemyHealthZero;

        RunEffectOnZero();
    }

    protected virtual void RunEffectOnZero()
    {
        Instantiate(_enemyDeathEffect, _effectSpawnPosition.position, Quaternion.identity);
        Destroy(_enemyBody);

    }

    #endregion
}