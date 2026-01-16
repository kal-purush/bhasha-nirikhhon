using UnityEngine;

[CreateAssetMenu(fileName = "PawnData", menuName = "PawnData", order = 1)]
public class PawnData : ScriptableObject
{
    public string PawnName;

    public int Hp;

    public int Speed;

    public AnimationClip AnimUp;
    public AnimationClip AnimDown;
    public AnimationClip AnimRight;
    public AnimationClip AnimLeft;
}