#nullable enable
using UnityEngine;

namespace Daipan.Stream.MonoScripts
{
    public class CommentMono : MonoBehaviour
    {
        [SerializeField] GameObject commentPrefab = null!;
        [SerializeField] GameObject commentSection = null!;
        [SerializeField] GameObject commentSectionHead = null!;
        [SerializeField] GameObject spawnPoint = null!;
        [SerializeField] GameObject despawnPoint = null!;

        GameObject? _commentUnit;
        [SerializeField] float commentSpeed = 0.01f;
        
        void Update()
        {
            if (Input.GetKeyDown(KeyCode.Space))
            {
                _commentUnit = Object.Instantiate(commentPrefab,spawnPoint.transform.position ,Quaternion.identity, commentSection.transform);
            }

            if (_commentUnit != null)
            {
                _commentUnit.transform.position += Vector3.up * commentSpeed;
                
                if (_commentUnit.transform.position.y > despawnPoint.transform.position.y)
                {
                    Object.Destroy(_commentUnit);
                }
            }
        }
    }
}