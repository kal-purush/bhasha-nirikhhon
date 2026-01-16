using UnityEngine;
using System.Collections;

namespace CRAG 
{
    /// <summary>
    /// Объект будет разлетаться на кусочки!
    /// </summary>
    public class DestructurableObject : MonoBehaviour
    {
        /// <summary>Префаб осколков</summary>
        public Transform shards;

        public void Boom()
        {
            Transform instance = Instantiate(shards, transform.position, Quaternion.identity) as Transform;

            foreach (Transform shard in instance)
            {
                shard.GetComponent<Rigidbody>()
                     .AddForce(new Vector3(Random.Range(-1f, 1f),
                                           Random.Range(-1f, 1f), 
                                           Random.Range(-1f, 1f)));
            }
        }
    }
}
