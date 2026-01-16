using UnityEngine;

public class CharacterState : MonoBehaviour
{
    Transform transform;

    public int WalkSpeed;
    public int RunSpeed;
    public Vector2 Velocity { get; set; }

    void Awake()
    {
        transform = gameObject.transform;
    }

    void FixedUpdate()
    {
        transform.Translate(Velocity);
    }
}