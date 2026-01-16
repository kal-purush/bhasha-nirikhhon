using UnityEngine;
using Glib.HitSupport;

namespace TeamB_TD
{
    namespace Battle
    {
        namespace Unit
        {
            namespace Enemy
            {
                public class ForwardObjectScanner : MonoBehaviour
                {
                    [SerializeField]
                    private Raycast2D _raycast2D;

                    public bool IsExistObject => _raycast2D.IsHit(transform);

                    public void SetDir(Vector2 dir)
                    {
                        _raycast2D.Dir = dir;
                    }

#if UNITY_EDITOR
                    private void OnDrawGizmos()
                    {
                        _raycast2D.OnDrawGizmos(transform);
                    }
#endif
                }
            }
        }
    }
}