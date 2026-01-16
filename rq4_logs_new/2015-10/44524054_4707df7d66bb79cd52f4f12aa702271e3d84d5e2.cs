using UnityEngine;
using System.Collections;

public class IconMover : MonoBehaviour
{
    Vector3 _enemyPos;

    void Start()
    {
    }

    void SetEnemyPos(Vector3 enemyPos_)
    {
        _enemyPos.Set(
            enemyPos_.x,
            enemyPos_.y,
            enemyPos_.z);
    }

    void Update()
    {

        for (var i = 0; i < 3; ++i)
        {

            if (transform.localPosition.x < _enemyPos.x)

                iTween.MoveTo(gameObject, _enemyPos, 1);
        }

    }

}