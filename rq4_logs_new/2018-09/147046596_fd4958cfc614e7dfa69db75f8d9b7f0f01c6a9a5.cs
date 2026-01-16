using UnityEngine;
using Unity.Entities;

[UpdateAfter(typeof(UnityEngine.Experimental.PlayerLoop.FixedUpdate))]
public class MoveSystem : ComponentSystem
{
    public struct Data
    {
        public readonly int Length;
        public EntityArray Entities;
        public GameObjectArray GameObjects;
        public ComponentArray<MoveComponent> MoveComponents;
        public ComponentArray<Rigidbody2D> rb2ds;
        public ComponentArray<Animator> animators;
    }

    [Inject] private Data data;

    protected override void OnUpdate()
    {
        for (int i = 0; i < data.Length; i++)
        {
            var gameObject = data.GameObjects[i];
            if (gameObject.tag == "Player")
            {
                Debug.Log("Player!");
            }
            else
            {
                Debug.Log("Enemy!");
            }

            Debug.Log(data.rb2ds[i].position);
        }
    }
}