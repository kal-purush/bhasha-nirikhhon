using UnityEngine;

static public class Layouts{

    readonly static private Vector2[] handScales={new Vector2(1.5f,1.5f),new Vector2(0.75f,0.75f)};
    readonly static private Vector2[] handOffsets = { new Vector2(-7f, -4f) ,new Vector2(-7f,4.5f),
    new Vector2(3.5f,4.5f),new Vector2(7f,-2f)};
    readonly static private Vector2[] handLineupDirections = { new Vector2(1f, 0f),new Vector2(0f,-0.5f) ,
    new Vector2(-0.5f,0f) ,new Vector2(0f,0.5f) };
    readonly static private Vector3[] handRotations = { new Vector3(0f, 0f, 0f),new Vector3(0f,0f,-90f),
    new Vector3(0f,0f,180f),new Vector3(0f,0f,90f)};

    static public Vector2 GetHandScale(int player)
    {
        if (player == 0)
        {
            return handScales[0];
        }
        else
        {
            return handScales[1];
        }
    }

    static public Vector2 GetHandOffset(int player)
    {
        return handOffsets[player];
    }

    static public Vector2 GetHandLineupDirection(int player)
    {
        return handLineupDirections[player];
    }

    static public Vector3 GetHandRotation(int player)
    {
        return handRotations[player];
    }
}