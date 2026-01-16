using TeamB_TD;
using UnityEngine;

namespace TeamB_TD
{
    namespace Battle
    {
        namespace Unit
        {
            namespace Ally
            {
                public class AttackRangeSetter : MonoBehaviour
                {
                    [SerializeField]
                    private Transform _transform;
                    [SerializeField]
                    private AllyParameter _parameter;

                    private void Start()
                    {
                        _transform.localScale = new Vector3(_parameter.AttackRange * 2f, _parameter.AttackRange * 2f, 1f);
                    }
                }
            }
        }
    }
}